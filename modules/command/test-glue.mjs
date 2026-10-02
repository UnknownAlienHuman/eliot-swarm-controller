// Self-test for modules/command: glue parser, describe, and open/snapshot
// against the fixture executable (fixtures/fake-cmd.mjs), which loads the
// real pinned mod (mod/eliot-command.ts) through a documented-shape ModApi
// host. No vendor binary, account, or model call is involved.
//
// Run: node test-glue.mjs   (or: npm test)

import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  classifyLine,
  describe,
  openRun,
  snapshotRun,
  DEFAULT_MOD_PATH,
} from "./glue.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE = join(HERE, "fixtures", "fake-cmd.mjs");
const config = {
  command: process.execPath,
  argsPrefix: [FAKE],
  modPath: DEFAULT_MOD_PATH,
};

const scratch = mkdtempSync(join(tmpdir(), "eliot-command-test-"));
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
    assert.equal(classified.filter((c) => c.kind === "result").length, 1);
    assert.equal(classified.find((c) => c.kind === "result").result.subtype, "success");
    assert.equal(classifyLine("not json").kind, "gap");
    assert.equal(classifyLine('{"answer": 42}').kind, "gap");
  });

  await test("describe reports the fixture CLI and the pinned mod honestly", async () => {
    const info = await describe(config);
    assert.match(info.cli_version, /1\.66\.0/);
    assert.match(info.mod.sha256, /^[0-9a-f]{64}$/);
    assert.equal(info.installed_runtime_verified, false);
    assert.equal(info.capabilities.goal, "unavailable_headless_setter_not_established");
    assert.equal(info.capabilities.resume, "unavailable_in_this_slice");
  });

  await test("open: success run, mod ready, queue admission is not application", async () => {
    const dir = join(scratch, "success");
    writeInbox(dir, [
      { id: "q1", kind: "queue_message", content: "also check the tests", deliverAs: "follow-up" },
      { id: "t1", kind: "set_active_tools", names: ["read", "search"] },
      { id: "m1", kind: "set_model", model: "fixture-model" },
    ]);
    process.env.FAKE_CMD_SCENARIO = "success";
    const record = await openRun(config, { prompt: "read the readme", controlDir: dir });
    assert.equal(record.disposition, "completed");
    assert.equal(record.disposition_basis, "result_line");
    assert.equal(record.exit.code, 0);
    assert.equal(record.exit.meaning, "EXIT_SUCCESS");
    assert.equal(record.session_id, "ses_fixture_1");
    assert.equal(record.session_id_source, "result_line");
    assert.equal(record.result.final_text, "done");
    assert.deepEqual(record.result.usage, { inputTokens: 120, outputTokens: 40 });
    assert.equal(record.mod.loaded, true);
    assert.equal(record.mod.ready, true);
    assert.equal(record.mod.session_ended, true);
    assert.equal(record.events.by_type.tool_completed, 1);
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
    assert.equal(snapshot.terminal, "completed");
    assert.equal(snapshot.completeness, "partial");
    assert.equal(snapshot.scope, "single_headless_run");
  });

  await test("open: auth error is a failed run with no session and no mod readiness", async () => {
    const dir = join(scratch, "auth");
    process.env.FAKE_CMD_SCENARIO = "auth-error";
    const record = await openRun(config, { prompt: "hi", controlDir: dir });
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
    const record = await openRun(config, { prompt: "hi", controlDir: dir });
    assert.equal(record.disposition, "max_turns");
    assert.equal(record.exit.code, 8);
    assert.equal(record.exit.meaning, "EXIT_MAX_TURNS_REACHED");
    assert.equal(record.result.stop_reason, "max_turns");
  });

  await test("open: a mod_error event is not process death", async () => {
    const dir = join(scratch, "mod-error");
    process.env.FAKE_CMD_SCENARIO = "mod-error";
    const record = await openRun(config, { prompt: "hi", controlDir: dir });
    assert.equal(record.disposition, "completed");
    assert.equal(record.exit.code, 0);
    assert.equal(record.mod.stream_mod_errors.length, 1);
    assert.equal(record.mod.stream_mod_errors[0].modId, "other-mod");
  });

  await test("open: process death without a result line stays unknown", async () => {
    const dir = join(scratch, "crash");
    process.env.FAKE_CMD_SCENARIO = "crash-no-result";
    const record = await openRun(config, { prompt: "hi", controlDir: dir });
    assert.equal(record.disposition, "unknown");
    assert.equal(record.disposition_basis, "missing_result_line");
    assert.equal(record.exit.code, 1);
    assert.equal(record.result, null);
  });

  await test("open: success may legitimately carry no session id", async () => {
    const dir = join(scratch, "no-session");
    process.env.FAKE_CMD_SCENARIO = "no-session-id";
    const record = await openRun(config, { prompt: "hi", controlDir: dir });
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
} finally {
  delete process.env.FAKE_CMD_SCENARIO;
  rmSync(scratch, { recursive: true, force: true });
}

process.stdout.write(`\n${passed} command glue tests passed\n`);

function writeInbox(dir, commands) {
  // Pre-seed the control directory before the native process starts so the
  // mod's inbox poll can consume the commands mid-run.
  mkdirSync(dir, { recursive: true });
  writeFileSync(
    join(dir, "inbox.ndjson"),
    commands.map((c) => JSON.stringify(c)).join("\n") + "\n",
    "utf8",
  );
}
