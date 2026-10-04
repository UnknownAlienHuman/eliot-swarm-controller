// Self-test for modules/command: glue parser, describe, and open/snapshot
// against the fixture executable (fixtures/fake-cmd.mjs), which loads the
// real pinned mod (mod/eliot-command.ts) through a documented-shape ModApi
// host. No vendor binary, account, or model call is involved.
//
// Run: node test-glue.mjs   (or: npm test)

import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyLine,
  MODULE_ARTIFACT_ID,
  PREVIOUS_MODULE_ARTIFACT_ID,
  commandReceiptFacts,
  describe,
  openRun,
  snapshotRun,
  DEFAULT_MOD_PATH,
  batchRunId,
  buildTaskPrompt,
  controlRecordRef,
  outcomeFromRun,
  resultRecordRef,
  sha256Hex,
} from "./glue.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE = join(HERE, "fixtures", "fake-cmd.mjs");
const config = {
  command: process.execPath,
  commandArgs: [FAKE],
  cwd: process.cwd(),
  modPath: DEFAULT_MOD_PATH,
};

const scratch = mkdtempSync(join(tmpdir(), "eliot-command-test-"));
const envProbeFile = join(scratch, "native-env.ndjson");
const ownerDir = join(scratch, "module-owner");
mkdirSync(ownerDir, { recursive: true });
writeFileSync(join(ownerDir, "owner.json"), JSON.stringify({
  version: 1,
  token: "owner-token-fixture",
  process: { purpose: "module" },
}));
process.env.ELIOT_SWARM_MODULE_STATE = ownerDir;
process.env.ELIOT_SWARM_MODULE_OWNER = join(ownerDir, "owner.json");
process.env.ELIOT_COMMAND_CONTROL_DIR = "parent-private-control-dir";
process.env.SWARM_QUAL_CAPTURE_ROOT = "private-qualification-root";
process.env.CAPTURE_SECRET_SENTINEL = "must-not-reach-native-child";
process.env.ANTHROPIC_API_KEY = "vendor-auth-sentinel";
process.env.FAKE_CMD_ENV_PROBE_FILE = envProbeFile;
let passed = 0;
async function test(name, fn) {
  await fn();
  passed += 1;
  process.stdout.write(`ok ${passed} - ${name}\n`);
}

try {
  await test("parser classifies the recorded stream by documented shape", () => {
    const lines = readFileSync(join(HERE, "fixtures", "recorded-success.ndjson"), "utf8")
      .split("\n")
      .filter((l) => l.trim() !== "");
    const classified = lines.map(classifyLine);
    assert.equal(classified.filter((c) => c.kind === "event").length, 7);
    assert.ok(classified.filter((c) => c.kind === "event")
      .every((c) => c.frame_format === "event_envelope" && c.raw_line.startsWith('{"type":"event"')));
    assert.equal(classified[0].event.type, "run_start");
    const legacyDirect = classifyLine('{"type":"run_start","sessionId":"legacy"}');
    assert.equal(legacyDirect.kind, "event");
    assert.equal(legacyDirect.frame_format, "direct_event_legacy");
    assert.equal(legacyDirect.event.sessionId, "legacy");
    assert.equal(classified.filter((c) => c.kind === "result").length, 1);
    assert.equal(classified.find((c) => c.kind === "result").result.subtype, "success");
    assert.equal(classified.find((c) => c.kind === "result").frame_format, "result_line");
    assert.equal(classifyLine('{"type":"event","event":{"type":7}}').reason, "invalid_event_frame");
    assert.equal(classifyLine("not json").kind, "gap");
    assert.equal(classifyLine('{"answer": 42}').kind, "gap");
  });

  await test("core-canonical prompt order and Unicode byte digest are stable", () => {
    const snapshot = { objective: "Review café 🐇", nested: { b: 2, a: "雪" } };
    const canonical = '{"nested":{"a":"雪","b":2},"objective":"Review café 🐇"}';
    const text = "Keep these words exactly: naïve 🐇";
    const prompt = buildTaskPrompt(snapshot, text, canonical);
    assert.equal(prompt, `${text}\n\nELIOT immutable task snapshot:\n${canonical}`);
    const facts = commandReceiptFacts("operation-🐇", prompt);
    assert.equal(facts.prompt_bytes, Buffer.byteLength(prompt, "utf8"));
    assert.equal(facts.prompt_sha256, sha256Hex(prompt));
    assert.throws(() => buildTaskPrompt(snapshot, text, '{"objective":"altered"}'), /CANONICAL_TASK_SNAPSHOT_MISMATCH/);
    const changedPrompt = buildTaskPrompt(snapshot, "changed words 🐇", canonical);
    assert.notEqual(commandReceiptFacts("operation-🐇", changedPrompt).prompt_sha256, facts.prompt_sha256);
  });

  await test("describe reports the fixture CLI and the pinned mod honestly", async () => {
    const info = await describe(config);
    assert.match(info.cli_version, /1\.66\.0/);
    assert.match(info.mod.sha256, /^[0-9a-f]{64}$/);
    assert.equal(info.installed_runtime_verified, false);
    assert.equal(info.capabilities.goal, "unavailable");
    assert.equal(info.capabilities.resume, "unavailable");
    assert.equal(info.capabilities.open, "executor_preflight_only_no_native_session");
    assert.equal(info.capabilities.task_dispatch, "one_shot_sessionless_batch");
    const versionProbe = JSON.parse(readFileSync(envProbeFile, "utf8").trim());
    assert.equal(versionProbe.owner_present, false);
    assert.equal(versionProbe.state_present, false);
    assert.equal(versionProbe.command_control_dir, null);
    assert.equal(versionProbe.qual_capture_present, false);
    assert.equal(versionProbe.capture_present, false);
    assert.equal(versionProbe.path_present, true);
    assert.equal(versionProbe.vendor_auth_present, true);
  });

  await test("open: success run, mod ready, queue admission is not application", async () => {
    const dir = join(scratch, "success");
    writeInbox(dir, [
      { id: "q1", kind: "queue_message", content: "also check the tests", deliverAs: "follow-up" },
      { id: "t1", kind: "set_active_tools", names: ["read", "search"] },
      { id: "m1", kind: "set_model", model: "fixture-model" },
    ]);
    process.env.FAKE_CMD_SCENARIO = "success";
    const record = await openFixtureRun("success-op", "read the readme", dir);
    assert.equal(record.disposition, "completed", record.evidence_validation.diagnostic_code);
    assert.equal(record.disposition_basis, "result_line");
    assert.equal(record.exit.code, 0);
    assert.equal(record.exit.meaning, "EXIT_SUCCESS");
    assert.equal(record.session_id, "ses_fixture_1");
    assert.equal(record.session_id_source, "result_line");
    assert.equal(record.result.final_text, "done");
    assert.equal(record.operation_id, "success-op");
    assert.equal(record.requested_model, "fixture-model");
    assert.equal(record.native_request_model, "fixture-model");
    assert.equal(record.native_request_model_status, "observed");
    assert.deepEqual(record.native_request_model_evidence.map((event) => event.event_type), [
      "model_request_start",
      "model_request_end",
    ]);
    assert.equal(record.effective_model, null);
    assert.equal(record.effective_model_status, "unknown");
    assert.equal(record.prompt_bytes, Buffer.byteLength("read the readme", "utf8"));
    assert.match(record.stderr.text, /fixture observed --model fixture-model/);
    assert.deepEqual(record.result.usage, { inputTokens: 120, outputTokens: 40 });
    assert.equal(record.mod.loaded, true);
    assert.equal(record.mod.ready, true);
    assert.equal(record.mod.session_ended, true);
    assert.equal(record.events.by_type.tool_completed, 1);
    assert.equal(record.events.by_type.model_request_start, 1);
    assert.equal(record.events.by_type.model_request_end, 1);
    const nativeEventRecords = readFileSync(join(dir, "events.ndjson"), "utf8")
      .trim().split(/\r?\n/).map(JSON.parse).filter((frame) => frame.kind === "event");
    assert.ok(nativeEventRecords.every((frame) => frame.frame_format === "event_envelope"
      && JSON.parse(frame.raw_line).event.type === frame.event.type));
    assert.equal(existsSync(join(dir, "mod", "mod-journal.ndjson")), true);
    assert.equal(existsSync(join(dir, "mod", "inbox.ndjson")), true);
    assert.equal(existsSync(join(dir, "mod-journal.ndjson")), false);
    assert.equal(existsSync(join(dir, "inbox.ndjson")), false);
    const nativeEnv = readFileSync(envProbeFile, "utf8").trim().split(/\r?\n/).map(JSON.parse).at(-1);
    assert.equal(nativeEnv.owner_present, false);
    assert.equal(nativeEnv.state_present, false);
    assert.equal(nativeEnv.qual_capture_present, false);
    assert.equal(nativeEnv.capture_present, false);
    assert.equal(nativeEnv.command_control_dir, join(dir, "mod"));
    assert.equal(nativeEnv.path_present, true);
    assert.equal(nativeEnv.vendor_auth_present, true);
    // queueMessage returned void: admission was journaled, application never.
    const admission = record.mod.queue_admissions.find((r) => r.id === "q1");
    assert.ok(admission, "queue admission journaled by the mod");
    assert.ok(!("applied" in admission), "admission must not claim application");
    // Tool control carries the native getter readback.
    const readback = record.mod.tools_readbacks.find((r) => r.id === "t1");
    assert.ok(readback, "tools readback journaled by the mod");
    assert.deepEqual(readback.active_after, ["read", "search"]);
    // Model setter has no documented readback: stays requested/unknown.
    const snapshot = snapshotRun(dir);
    assert.equal(snapshot.terminal, "completed", snapshot.evidence.diagnostic_code);
    assert.equal(snapshot.completeness, "partial");
    assert.equal(snapshot.scope, "single_headless_run");
    assert.equal(snapshot.admission.operation_id, "success-op");
    const terminal = outcomeFromRun(record);
    assert.equal(terminal.outcome, "applied");
    assert.equal(terminal.details.completion_condition, "native_result_observed");
    assert.equal(terminal.details.batch_run_id, batchRunId("success-op"));
    assert.equal(terminal.details.native_session_id, "ses_fixture_1");
    assert.equal(terminal.details.effective_model_status, "unknown");
    assert.equal(terminal.details.control_record_ref, controlRecordRef("success-op"));
    assert.equal(terminal.details.result_ref, resultRecordRef("success-op"));

    const replay = await openFixtureRun("success-op", "read the readme", dir);
    assert.equal(replay.replayed_from_saved_evidence, true);
    assert.equal(replay.started_at, record.started_at);
    await assert.rejects(
      () => openFixtureRun("success-op", "changed prompt", dir),
      /OPERATION_ID_CONFLICT/,
    );
    const mismatch = outcomeFromRun({
      ...record,
      exit: { ...record.exit, code: 1 },
      anomalies: ["exit_result_mismatch"],
    });
    assert.equal(mismatch.outcome, "unknown");
    assert.equal(mismatch.details.exit_code, 1);
    assert.equal("completion_condition" in mismatch.details, false);
  });

  await test("reconcile rejects a saved result projection that disagrees with its raw frame and does not replay", async () => {
    const dir = join(scratch, "tampered-result-stream");
    const invocationFile = join(scratch, "tampered-result-invocations.ndjson");
    process.env.FAKE_CMD_SCENARIO = "success";
    process.env.FAKE_CMD_INVOCATION_FILE = invocationFile;
    const record = await openFixtureRun("tampered-result-op", "read the fixture", dir);
    assert.equal(record.evidence_validation.valid, true);
    const stream = readFileSync(join(dir, "events.ndjson"), "utf8").trim().split(/\r?\n/).map(JSON.parse);
    const resultFrame = stream.find((frame) => frame.kind === "result");
    resultFrame.result.finalText = "altered after the run";
    writeFileSync(join(dir, "events.ndjson"), stream.map((frame) => JSON.stringify(frame)).join("\n") + "\n");
    const snapshot = snapshotRun(dir);
    assert.equal(snapshot.evidence.valid, false);
    assert.equal(snapshot.evidence.diagnostic_code, "saved_native_frame_projection_mismatch");
    assert.equal(snapshot.terminal, "unknown");
    const replay = await openFixtureRun("tampered-result-op", "read the fixture", dir);
    assert.equal(replay.replayed_from_saved_evidence, true);
    assert.equal(replay.evidence_validation.valid, false);
    assert.equal(outcomeFromRun(replay).outcome, "unknown");
    assert.equal(readFileSync(invocationFile, "utf8").trim().split(/\r?\n/).length, 1);
    delete process.env.FAKE_CMD_INVOCATION_FILE;
  });

  await test("reconcile rejects terminal exit facts that disagree with the retained result and does not replay", async () => {
    const dir = join(scratch, "tampered-exit-facts");
    const invocationFile = join(scratch, "tampered-exit-invocations.ndjson");
    process.env.FAKE_CMD_SCENARIO = "success";
    process.env.FAKE_CMD_INVOCATION_FILE = invocationFile;
    const record = await openFixtureRun("tampered-exit-op", "read the fixture", dir);
    assert.equal(record.evidence_validation.valid, true);
    const savedRun = JSON.parse(readFileSync(join(dir, "run.json"), "utf8"));
    savedRun.exit.code = 4;
    savedRun.exit.meaning = "EXIT_PERMISSION_DENIED";
    writeFileSync(join(dir, "run.json"), JSON.stringify(savedRun, null, 2) + "\n");
    const snapshot = snapshotRun(dir);
    assert.equal(snapshot.evidence.valid, false);
    assert.equal(snapshot.evidence.diagnostic_code, "saved_terminal_projection_mismatch");
    const replay = await openFixtureRun("tampered-exit-op", "read the fixture", dir);
    assert.equal(replay.replayed_from_saved_evidence, true);
    assert.equal(outcomeFromRun(replay).outcome, "unknown");
    assert.equal(readFileSync(invocationFile, "utf8").trim().split(/\r?\n/).length, 1);
    delete process.env.FAKE_CMD_INVOCATION_FILE;
  });

  await test("a mismatched core binding is rejected before admission or native spawn", async () => {
    const dir = join(scratch, "bad-core-binding");
    const prompt = "unaltered prompt 🐇";
    const invocationFile = join(scratch, "bad-core-binding-invocations.ndjson");
    process.env.FAKE_CMD_INVOCATION_FILE = invocationFile;
    await assert.rejects(() => openRun(config, {
      operationId: "bad-core-binding-op",
      requestedModel: "fixture-model",
      prompt,
      coreBinding: {
        ...commandReceiptFacts("bad-core-binding-op", prompt),
        prompt_sha256: sha256Hex("altered prompt 🐇"),
      },
      controlDir: dir,
    }), /CORE_PROMPT_BINDING_MISMATCH/);
    assert.equal(existsSync(join(dir, "admission.json")), false);
    assert.equal(existsSync(invocationFile), false);
    delete process.env.FAKE_CMD_INVOCATION_FILE;
  });

  await test("legacy .2 files stay readable and unchanged but cannot authorize .4 replay", async () => {
    const dir = join(scratch, "legacy-v2");
    const operationId = "legacy-v2-op";
    mkdirSync(dir, { recursive: true });
    const oldAdmission = {
      schema: 1,
      operation_id: operationId,
      execution_shape: "sessionless_batch",
      batch_run_id: batchRunId(operationId),
      requested_model: "fixture-model",
      prompt_sha256: sha256Hex("legacy prompt"),
      prompt_bytes: Buffer.byteLength("legacy prompt", "utf8"),
      control_record_ref: controlRecordRef(operationId),
      result_ref: resultRecordRef(operationId),
      artifact_refs: [
        { kind: "command_control_record", ref: controlRecordRef(operationId) },
        { kind: "command_result_record", ref: resultRecordRef(operationId) },
      ],
      admitted_at: "historical-fixture",
    };
    const oldRun = {
      schema: 1,
      module_artifact_id: "command-mod-0.1.0-glue.2",
      operation_id: operationId,
      disposition: "completed",
      events: { total: 0, by_type: {}, gaps: [] },
    };
    writeFileSync(join(dir, "admission.json"), JSON.stringify(oldAdmission, null, 2) + "\n");
    writeFileSync(join(dir, "run.json"), JSON.stringify(oldRun, null, 2) + "\n");
    writeFileSync(join(dir, "mod-journal.ndjson"), JSON.stringify({ kind: "mod_ready" }) + "\n");
    const admissionBefore = readFileSync(join(dir, "admission.json"), "utf8");
    const runBefore = readFileSync(join(dir, "run.json"), "utf8");
    const historical = snapshotRun(dir);
    assert.equal(historical.run.disposition, "completed");
    assert.equal(historical.terminal, "unknown");
    assert.equal(historical.evidence.diagnostic_code, "legacy_artifact_read_only");
    assert.equal(historical.mod.ready, true);
    await assert.rejects(() => openFixtureRun(operationId, "legacy prompt", dir), /OPERATION_ID_CONFLICT/);
    assert.equal(readFileSync(join(dir, "admission.json"), "utf8"), admissionBefore);
    assert.equal(readFileSync(join(dir, "run.json"), "utf8"), runBefore);
  });

  await test(".3 snapshots project saved event envelopes read-only", async () => {
    const dir = join(scratch, "legacy-v3-event-projection");
    const operationId = "legacy-v3-event-projection-op";
    mkdirSync(dir, { recursive: true });
    const eventRecord = {
      seq: 1,
      kind: "event",
      // .3 saved the parsed outer event frame as the event itself.
      event: {
        type: "event",
        event: { type: "model_request_start", model: "stealth/space-bunny-alpha" },
      },
    };
    writeFileSync(join(dir, "admission.json"), JSON.stringify({
      schema: 2,
      module_artifact_id: PREVIOUS_MODULE_ARTIFACT_ID,
      operation_id: operationId,
    }, null, 2) + "\n");
    writeFileSync(join(dir, "run.json"), JSON.stringify({
      schema: 2,
      module_artifact_id: PREVIOUS_MODULE_ARTIFACT_ID,
      disposition: "completed",
      events: { total: 1, by_type: { event: 1 }, gaps: [] },
    }, null, 2) + "\n");
    writeFileSync(join(dir, "events.ndjson"), JSON.stringify(eventRecord) + "\n");
    const savedBefore = ["admission.json", "run.json", "events.ndjson"]
      .map((name) => readFileSync(join(dir, name), "utf8"));
    const snapshot = snapshotRun(dir);
    assert.equal(snapshot.terminal, "unknown");
    assert.equal(snapshot.evidence.diagnostic_code, "legacy_artifact_read_only");
    assert.equal(snapshot.event_projection.read_only, true);
    assert.equal(snapshot.event_projection.raw_line_available, false);
    assert.deepEqual(snapshot.event_projection.by_type, { model_request_start: 1 });
    assert.equal(snapshot.event_projection.native_request_model, "stealth/space-bunny-alpha");
    assert.equal(snapshot.run.events.by_type.event, 1);
    assert.deepEqual(["admission.json", "run.json", "events.ndjson"]
      .map((name) => readFileSync(join(dir, name), "utf8")), savedBefore);
    await assert.rejects(
      () => openFixtureRun(operationId, "do not replay", dir),
      /OPERATION_ID_CONFLICT/,
    );
  });

  await test("open: auth error is a failed run with no session and no mod readiness", async () => {
    const dir = join(scratch, "auth");
    process.env.FAKE_CMD_SCENARIO = "auth-error";
    const record = await openFixtureRun("auth-op", "hi", dir);
    assert.equal(record.disposition, "failed");
    assert.equal(record.exit.code, 3);
    assert.equal(record.exit.meaning, "EXIT_AUTH_ERROR");
    assert.equal(record.session_id, null);
    assert.equal(record.result.error, "not authenticated");
    assert.equal(record.mod.loaded, true); // factory ran
    assert.equal(record.mod.ready, false); // session never bound
  });

  await test("open: max_turns is its own disposition, not success or failure", async () => {
    const dir = join(scratch, "max-turns");
    process.env.FAKE_CMD_SCENARIO = "max-turns";
    const record = await openFixtureRun("max-turns-op", "hi", dir);
    assert.equal(record.disposition, "max_turns");
    assert.equal(record.exit.code, 8);
    assert.equal(record.exit.meaning, "EXIT_MAX_TURNS_REACHED");
    assert.equal(record.result.stop_reason, "max_turns");
  });

  await test("open: a mod_error event is not process death", async () => {
    const dir = join(scratch, "mod-error");
    process.env.FAKE_CMD_SCENARIO = "mod-error";
    const record = await openFixtureRun("mod-error-op", "hi", dir);
    assert.equal(record.disposition, "completed");
    assert.equal(record.exit.code, 0);
    assert.equal(record.mod.stream_mod_errors.length, 1);
    assert.equal(record.mod.stream_mod_errors[0].modId, "other-mod");
  });

  await test("open: process death without a result line stays unknown", async () => {
    const dir = join(scratch, "crash");
    process.env.FAKE_CMD_SCENARIO = "crash-no-result";
    const record = await openFixtureRun("crash-op", "hi", dir);
    assert.equal(record.disposition, "unknown");
    assert.equal(record.disposition_basis, "missing_result_line");
    assert.equal(record.exit.code, 1);
    assert.equal(record.result, null);
  });

  await test("open: success result with a conflicting exit remains unknown", async () => {
    const dir = join(scratch, "exit-mismatch");
    process.env.FAKE_CMD_SCENARIO = "success-exit-mismatch";
    const record = await openFixtureRun("exit-mismatch-op", "hi", dir);
    assert.equal(record.result.subtype, "success");
    assert.equal(record.exit.code, 1);
    assert.equal(record.disposition, "unknown");
    assert.equal(record.disposition_basis, "result_exit_or_stream_mismatch");
    assert.deepEqual(record.anomalies, ["exit_result_mismatch"]);
    const outcome = outcomeFromRun(record);
    assert.equal(outcome.outcome, "unknown");
    assert.equal("completion_condition" in outcome.details, false);
  });

  await test("open: success may legitimately carry no session id", async () => {
    const dir = join(scratch, "no-session");
    process.env.FAKE_CMD_SCENARIO = "no-session-id";
    const record = await openFixtureRun("no-session-op", "hi", dir);
    assert.equal(record.disposition, "completed");
    assert.equal(record.session_id, null);
    assert.equal(record.session_id_source, "none");
  });

  await test("snapshot of an unused directory observes nothing", () => {
    const snapshot = snapshotRun(join(scratch, "empty"));
    assert.equal(snapshot.terminal, "not_observed");
    assert.equal(snapshot.run, null);
    assert.equal(snapshot.mod.loaded, false);
  });

  await test("admission without terminal evidence is read back as unknown without replay", async () => {
    const dir = join(scratch, "admitted-only");
    const prompt = "do not replay";
    const operationId = "admitted-only-op";
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "admission.json"), JSON.stringify({
      schema: 2,
      module_artifact_id: MODULE_ARTIFACT_ID,
      operation_id: operationId,
      execution_shape: "sessionless_batch",
      batch_run_id: batchRunId(operationId),
      requested_model: "fixture-model",
      prompt_sha256: sha256Hex(prompt),
      prompt_bytes: Buffer.byteLength(prompt, "utf8"),
      core_binding: commandReceiptFacts(operationId, prompt),
      control_record_ref: controlRecordRef(operationId),
      result_ref: resultRecordRef(operationId),
      artifact_refs: [
        { kind: "command_control_record", ref: controlRecordRef(operationId) },
        { kind: "command_result_record", ref: resultRecordRef(operationId) },
      ],
      admitted_at: "fixture",
    }));
    const record = await openFixtureRun(operationId, prompt, dir);
    assert.equal(record.disposition, "unknown");
    assert.equal(record.disposition_basis, "admission_without_terminal_record");
    assert.equal(record.replayed_from_saved_evidence, true);
    assert.equal(record.anomalies[0], "native_result_missing_after_admission");
    assert.equal(snapshotRun(dir).run, null);
  });
} finally {
  delete process.env.FAKE_CMD_SCENARIO;
  delete process.env.FAKE_CMD_ENV_PROBE_FILE;
  delete process.env.FAKE_CMD_INVOCATION_FILE;
  delete process.env.ELIOT_SWARM_MODULE_STATE;
  delete process.env.ELIOT_SWARM_MODULE_OWNER;
  delete process.env.ELIOT_COMMAND_CONTROL_DIR;
  delete process.env.SWARM_QUAL_CAPTURE_ROOT;
  delete process.env.CAPTURE_SECRET_SENTINEL;
  delete process.env.ANTHROPIC_API_KEY;
  rmSync(scratch, { recursive: true, force: true });
}

process.stdout.write(`\n${passed} command glue tests passed\n`);

function writeInbox(dir, commands) {
  // Pre-seed the control directory before the native process starts so the
  // mod's inbox poll can consume the commands mid-run.
  mkdirSync(join(dir, "mod"), { recursive: true });
  writeFileSync(
    join(dir, "mod", "inbox.ndjson"),
    commands.map((c) => JSON.stringify(c)).join("\n") + "\n",
    "utf8",
  );
}

function openFixtureRun(operationId, prompt, controlDir) {
  return openRun(config, {
    operationId,
    requestedModel: "fixture-model",
    prompt,
    coreBinding: commandReceiptFacts(operationId, prompt),
    controlDir,
  });
}
