import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { spawn } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { mkdtemp, lstat, readFile, realpath, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isExactLoopbackOrigin, validatePriorStopRecord } from "./serve.mjs";

const SERVER_VERSION = "2.0.7";
const USERNAME = "opencode";
const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const SERVE_SCRIPT = path.join(MODULE_DIR, "serve.mjs");
const READY_TIMEOUT_MS = 60_000;
const STOP_TIMEOUT_MS = 30_000;
const DIAGNOSTIC_STARTUP_TIMEOUT_MS = 15_000;

function authHeader(password) {
  return `Basic ${Buffer.from(`${USERNAME}:${password}`, "utf8").toString("base64")}`;
}

function parseArguments(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    if (key !== "--bun" || values.has(key) || !argv[index + 1]) {
      throw new Error("Usage: node selftest.mjs --bun <absolute-path-to-bun-1.4.0>");
    }
    values.set(key, argv[++index]);
  }
  const bun = values.get("--bun") ?? process.env.ELIOT_OPENCODE_BUN_EXE;
  if (!bun || !path.isAbsolute(bun)) {
    throw new Error("Provide the pinned Bun executable with --bun or ELIOT_OPENCODE_BUN_EXE");
  }
  return path.resolve(bun);
}

async function verifyRuntime(bunPath) {
  const info = await lstat(bunPath);
  assert.ok(info.isFile() && !info.isSymbolicLink(), "Bun path must be a regular executable file");
  assert.equal(await realpath(bunPath), bunPath, "Bun path must resolve directly without a redirect");
  const child = spawn(bunPath, ["--version"], { stdio: ["ignore", "pipe", "ignore"], windowsHide: true });
  const chunks = [];
  child.stdout.on("data", (chunk) => chunks.push(chunk));
  const result = await withTimeout(observeExit(child), 10_000, "Bun version check");
  assert.equal(result.code, 0, "the selected Bun executable must start successfully");
  assert.equal(Buffer.concat(chunks).toString("utf8").trim(), "1.4.0", "the service runtime must be Bun 1.4.0");
}

function observeExit(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve({ code: child.exitCode, signal: child.signalCode });
  }
  return new Promise((resolve, reject) => {
    child.once("error", (error) => reject(new Error(`Owned Bun process could not start (${error.code ?? "spawn error"})`)));
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });
}

function withTimeout(promise, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} did not exit within ${timeoutMs} ms`)), timeoutMs);
    promise.then((result) => {
      clearTimeout(timer);
      resolve(result);
    }, (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
}

function captureTail(stream, maxBytes = 32 * 1024) {
  const chunks = [];
  let byteCount = 0;
  stream.on("data", (chunk) => {
    const copy = Buffer.from(chunk);
    chunks.push(copy);
    byteCount += copy.length;
    while (byteCount > maxBytes && chunks.length > 0) {
      const excess = byteCount - maxBytes;
      if (chunks[0].length <= excess) byteCount -= chunks.shift().length;
      else {
        chunks[0] = chunks[0].subarray(excess);
        byteCount -= excess;
      }
    }
  });
  return () => Buffer.concat(chunks).toString("utf8");
}

function safeDiagnostics(parts, password) {
  return parts
    .filter(Boolean)
    .join("\n")
    .replaceAll(password, "[redacted-test-password]")
    .replace(/\b(authorization)\s*([:=])\s*[^\r\n]*/gi, "$1$2[redacted]")
    .replace(/\b(basic|bearer)\s+\S+/gi, "$1 [redacted]")
    .replace(/(authorization|password|token|api[_-]?key)\s*([=:])\s*[^\s,}\]]+/gi, "$1$2[redacted]")
    .replace(/\u001b\[[0-?]*[ -/]*[@-~]/g, "")
    .slice(-24 * 1024);
}

function verifySafetyFixtures() {
  const port = 12345;
  assert.equal(isExactLoopbackOrigin(`http://127.0.0.1:${port}`, port), true, "must accept the exact native loopback origin");
  assert.equal(isExactLoopbackOrigin(`http://127.0.0.1:${port}/api/info`, port), true, "must accept a path below the exact native loopback origin");
  for (const candidate of [
    `http://attacker.test/?127.0.0.1:${port}`,
    `http://127.0.0.1:${port}.attacker.test`,
    `http://user@127.0.0.1:${port}`,
    `https://127.0.0.1:${port}`,
    `http://127.0.0.1:${port + 1}`,
    `http://2130706433:${port}`,
  ]) {
    assert.equal(isExactLoopbackOrigin(candidate, port), false, `must reject non-exact ready origin: ${candidate}`);
  }

  const paths = {
    connectionPath: "C:\\private\\connection.json",
    stopPath: "C:\\private\\stop-receipt.json",
    databasePath: "C:\\private\\data\\opencode.sqlite",
  };
  const nonce = "fixture-owner-nonce";
  const completedAt = "2026-10-03T00:00:00.000Z";
  const previous = {
    schema_version: 1,
    owner_nonce: nonce,
    status: "stopped",
    runtime: "bun",
    runtime_version: "1.4.0",
    native_server: "@opencode/server",
    native_server_version: SERVER_VERSION,
    pid: 43210,
    database_path: paths.databasePath,
    connection_file: paths.connectionPath,
    stop_receipt: paths.stopPath,
    stopped_at: completedAt,
    listener_closed: true,
    runtime_disposed: true,
    connection_absent: true,
    server_fiber: "interrupted",
    process_exit_code: 0,
  };
  const receipt = {
    schema_version: 1,
    owner_nonce: nonce,
    pid: previous.pid,
    status: "stopped",
    reason: "fixture",
    completed_at: completedAt,
    listener_closed: true,
    runtime_disposed: true,
    connection_absent: true,
    server_fiber: "interrupted",
    process_exit_code: 0,
  };
  assert.doesNotThrow(() => validatePriorStopRecord(previous, receipt, paths));
  for (const invalidReceipt of [
    undefined,
    { ...receipt, schema_version: 2 },
    { ...receipt, status: "requested" },
    { ...receipt, owner_nonce: "other-owner" },
    { ...receipt, pid: previous.pid + 1 },
    { ...receipt, runtime_disposed: false },
    { ...receipt, connection_absent: false },
  ]) {
    assert.throws(() => validatePriorStopRecord(previous, invalidReceipt, paths), /do not prove one matching clean shutdown/);
  }
  assert.throws(() => validatePriorStopRecord(previous, receipt, { ...paths, databasePath: "C:\\other\\opencode.sqlite" }), /do not prove one matching clean shutdown/);

  const password = "synthetic-selftest-secret-value";
  const basic = authHeader(password);
  const basicPayload = basic.slice("Basic ".length);
  const bearerPayload = "test-bearer-token-123456789";
  const scrubbed = safeDiagnostics([
    `Authorization: ${basic}`,
    `Authorization: Bearer ${bearerPayload}`,
    `{"authorization":"${basic}"}`,
    `password=${password}`,
  ], password);
  for (const secret of [password, basic, basicPayload, bearerPayload]) {
    assert.equal(scrubbed.includes(secret), false, "diagnostic output must redact complete auth values and the known password");
  }
}

async function findFreePort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address === "object");
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return address.port;
}

async function readJson(target) {
  try {
    return JSON.parse(await readFile(target, "utf8"));
  } catch (error) {
    if (error?.code === "ENOENT" || error instanceof SyntaxError) return null;
    throw error;
  }
}

async function startService(bunPath, stateRoot, passwordFile, password, port, spawnedServices) {
  const child = spawn(bunPath, [
    SERVE_SCRIPT,
    "--state-root", stateRoot,
    "--password-file", passwordFile,
    "--port", String(port),
    "--model-catalog", "offline",
    "--ready-timeout-ms", String(DIAGNOSTIC_STARTUP_TIMEOUT_MS),
    "--stop-on-stdin-eof",
  ], {
    cwd: MODULE_DIR,
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true,
  });
  const stdout = captureTail(child.stdout);
  const stderr = captureTail(child.stderr);
  const exit = observeExit(child);
  const ownerPath = path.join(stateRoot, "owner.json");
  const connectionPath = path.join(stateRoot, "connection.json");
  const service = { child, exit, ownerPath, connectionPath, stopPath: path.join(stateRoot, "stop-receipt.json"), stdout, stderr };
  spawnedServices.add(service);
  const deadline = Date.now() + READY_TIMEOUT_MS;
  let owner;
  let lastHealth = null;
  const healthCounts = new Map();
  let nextHealthAt = 0;
  try {
    while (Date.now() < deadline) {
      owner = await readJson(ownerPath);
      if (owner?.bound_port && Date.now() >= nextHealthAt) {
        nextHealthAt = Date.now() + 500;
        try {
          const response = await fetch(`http://127.0.0.1:${owner.bound_port}/api/info`, {
            headers: { authorization: authHeader(password) },
            redirect: "error",
            signal: AbortSignal.timeout(1000),
          });
          const contentType = response.headers.get("content-type") ?? "";
          const body = /json/i.test(contentType) ? await response.json().catch(() => null) : null;
          lastHealth = { status: response.status, code: body?.code ?? null, version: body?.version ?? null, pid: body?.pid ?? null };
          healthCounts.set(response.status, (healthCounts.get(response.status) ?? 0) + 1);
        } catch (error) {
          lastHealth = { status: "request-error", cause: error?.cause?.code ?? error?.name ?? "unknown" };
        }
      }
      if (child.exitCode !== null || child.signalCode !== null) {
        const result = await exit;
        throw new Error(`OpenCode owner exited before readiness (code=${result.code ?? "none"}, signal=${result.signal ?? "none"}); health=${JSON.stringify(lastHealth)}; status_counts=${JSON.stringify(Object.fromEntries(healthCounts))}`);
      }
      if (owner?.status === "ready") break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    assert.equal(owner?.status, "ready", "native owner metadata must reach ready state");
    assert.equal(owner.native_server_version, SERVER_VERSION);
    assert.equal(owner.pid, child.pid, "owner metadata must identify the exact spawned Bun process");
    assert.equal(owner.requested_port, port);
    assert.equal(owner.bound_port, port);
    assert.match(owner.endpoint, new RegExp(`^http://127\\.0\\.0\\.1:${port}$`));
    assert.equal(path.resolve(owner.database_path), path.join(stateRoot, "data", "opencode.sqlite"));
    const connection = await readJson(connectionPath);
    assert.equal(connection?.schema_version, 1);
    assert.equal(connection?.endpoint, owner.endpoint);
    assert.equal(connection?.pid, child.pid);
    assert.equal(connection?.username, USERNAME);
    assert.equal(connection?.password, await readFile(passwordFile, "utf8").then((value) => value.trim()));
    service.owner = owner;
    return service;
  } catch (error) {
    const diagnostics = safeDiagnostics([stderr(), stdout()], password);
    await writeFile(path.join(stateRoot, "startup-diagnostics.log"), `${error instanceof Error ? error.message : "startup error"}\n${diagnostics}\n`, { flag: "wx" }).catch(() => {});
    child.stdin.end();
    try {
      await withTimeout(exit, STOP_TIMEOUT_MS, "OpenCode owner cleanup after startup failure");
      spawnedServices.delete(service);
    } catch {
      // Preserve the test state if an exact owner process cannot be proven stopped.
    }
    throw error;
  }
}

async function stopService(service, spawnedServices) {
  service.child.stdin.end();
  const result = await withTimeout(service.exit, STOP_TIMEOUT_MS, "OpenCode owner shutdown");
  assert.equal(result.code, 0, "the exact owned Bun process must exit cleanly after stdin EOF");
  assert.equal(result.signal, null, "clean owner shutdown must not be reported as a process signal");
  const [owner, stopReceipt] = await Promise.all([readJson(service.ownerPath), readJson(service.stopPath)]);
  assert.equal(owner?.status, "stopped", "owner metadata must record completed shutdown");
  assert.equal(owner?.process_exit_code, result.code, "owner metadata must match the actual process exit code");
  assert.equal(stopReceipt?.status, "stopped", "stop receipt must record completed shutdown");
  assert.equal(stopReceipt?.pid, service.child.pid, "stop receipt must identify the exact spawned service");
  assert.equal(stopReceipt?.process_exit_code, result.code, "stop receipt must match the observed process exit code");
  assert.equal(stopReceipt?.owner_nonce, owner?.owner_nonce, "stop receipt and owner must share one nonce");
  assert.equal(stopReceipt?.runtime_disposed, true, "native runtime disposal must finish before shutdown is reported");
  assert.equal(stopReceipt?.connection_absent, true, "the service connection record must be absent before stopped status");
  assert.equal(stopReceipt?.listener_closed, true, "stopped receipt must prove the listener closed");
  assert.equal(owner?.runtime_disposed, true);
  assert.equal(await readJson(service.connectionPath), null, "shutdown must remove only its matching private connection record");
  spawnedServices.delete(service);
}

async function request(url, options = {}) {
  const response = await fetch(url, { redirect: "error", signal: AbortSignal.timeout(10_000), ...options });
  if (!response.ok) {
    const status = response.status;
    await response.body?.cancel().catch(() => {});
    throw new Error(`Local OpenCode smoke request failed with HTTP ${status}`);
  }
  return response;
}

function parseEventStream(body) {
  const result = [];
  for (const frame of body.split(/\r?\n\r?\n/)) {
    const payload = frame.split(/\r?\n/).filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trim()).join("\n");
    if (!payload) continue;
    try {
      result.push(JSON.parse(payload));
    } catch {
      throw new Error("Persisted session log returned an invalid event frame");
    }
  }
  return result;
}

async function verifySessionLog(service, password, sessionId) {
  const response = await request(
    `${service.owner.endpoint}/api/experimental/session/${encodeURIComponent(sessionId)}/log?follow=false`,
    { headers: { authorization: authHeader(password) } },
  );
  assert.match(response.headers.get("content-type") ?? "", /text\/event-stream/i);
  const events = parseEventStream(await response.text());
  const created = events.filter((event) => event.type === "session.created" && event.data?.sessionID === sessionId);
  assert.equal(created.length, 1, "durable session log must contain exactly one matching session.created event");
  const event = created[0];
  assert.equal(event.durable?.aggregateID, sessionId, "session.created must be stored under the exact session aggregate");
  assert.equal(event.durable?.version, 1, "session.created must retain the pinned event schema version");
  assert.ok(typeof event.id === "string" && event.id.length > 0, "session.created must retain its native event ID");
  assert.ok(Number.isInteger(event.durable?.seq) && event.durable.seq >= 0, "session.created must retain its durable sequence");
  const watermarks = events.filter((candidate) => candidate.type === "log.synced" && candidate.aggregateID === sessionId);
  assert.ok(watermarks.some((mark) => Number.isInteger(mark.seq) && mark.seq >= event.durable.seq), "session log must end at a synced watermark covering session.created");
  return { eventId: event.id, sequence: event.durable.seq };
}

async function createModelFreeSession(service, password) {
  const unauthorized = await fetch(`${service.owner.endpoint}/api/info`, { redirect: "error", signal: AbortSignal.timeout(10_000) });
  assert.equal(unauthorized.status, 401, "server must reject an unauthenticated local client");
  await unauthorized.body?.cancel().catch(() => {});

  const health = await request(`${service.owner.endpoint}/api/info`, { headers: { authorization: authHeader(password) } });
  const info = await health.json();
  assert.equal(info.version, SERVER_VERSION);
  assert.equal(info.pid, service.child.pid);

  const createdResponse = await request(`${service.owner.endpoint}/api/session`, {
    method: "POST",
    headers: { authorization: authHeader(password), "content-type": "application/json" },
    body: JSON.stringify({ title: "No-model durable event smoke" }),
  });
  const createdBody = await createdResponse.json();
  const sessionId = createdBody?.data?.id;
  assert.ok(typeof sessionId === "string" && sessionId.length > 0, "native session creation must return an exact session ID");
  // The smoke issues no model, provider, prompt, execute, or inference request.
  return sessionId;
}

function verifyDatabase(dbPath, sessionId, expectedEventId, expectedSequence) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    const rows = db.prepare("SELECT id, aggregate_id, seq, type, json_extract(data, '$.sessionID') AS session_id FROM event WHERE aggregate_id = ? AND type = 'session.created.1'").all(sessionId);
    assert.equal(rows.length, 1, "private SQLite must contain exactly one durable session.created row");
    assert.equal(rows[0].id, expectedEventId);
    assert.equal(rows[0].aggregate_id, sessionId);
    assert.equal(rows[0].session_id, sessionId);
    assert.equal(rows[0].seq, expectedSequence);
  } finally {
    db.close();
  }
}

async function main() {
  verifySafetyFixtures();
  const bunPath = parseArguments(process.argv.slice(2));
  await verifyRuntime(bunPath);
  const stateRoot = await mkdtemp(path.join(os.tmpdir(), "eliot-opencode-persist-smoke-"));
  const rootReal = await realpath(stateRoot);
  const password = randomBytes(32).toString("base64url");
  const passwordFile = path.join(rootReal, "server.password");
  await writeFile(passwordFile, `${password}\n`, { flag: "wx", mode: 0o600 });

  let first;
  let second;
  const spawnedServices = new Set();
  let smokePassed = false;
  try {
    first = await startService(bunPath, rootReal, passwordFile, password, await findFreePort(), spawnedServices);
    const expectedDatabase = path.join(rootReal, "data", "opencode.sqlite");
    assert.equal(path.resolve(first.owner.database_path), expectedDatabase, "owner metadata must publish its exact private database path");
    assert.equal(first.owner.model_catalog_mode, "offline", "smoke must not load or fetch a model catalog");
    assert.equal(first.owner.model_catalog_policy, "no-snapshot-no-fetch");
    const sessionId = await createModelFreeSession(first, password);
    const expectedEvent = await verifySessionLog(first, password, sessionId);
    await stopService(first, spawnedServices);
    first = undefined;

    second = await startService(bunPath, rootReal, passwordFile, password, await findFreePort(), spawnedServices);
    assert.equal(path.resolve(second.owner.database_path), expectedDatabase);
    assert.equal(second.owner.model_catalog_mode, "offline");
    const recovered = await verifySessionLog(second, password, sessionId);
    assert.deepEqual(recovered, expectedEvent, "restart must preserve the exact native event identity and sequence");
    await stopService(second, spawnedServices);
    second = undefined;

    verifyDatabase(expectedDatabase, sessionId, expectedEvent.eventId, expectedEvent.sequence);
    smokePassed = true;
    console.log(`OpenCode ${SERVER_VERSION} persistence selftest: PASS (Node ${process.versions.node} harness, Bun 1.4.0 owner, authenticated loopback, model-free session.created, exact log readback after restart, read-only SQLite row, clean observed process exits)`);
  } finally {
    for (const service of [...spawnedServices]) {
      service.child.stdin.end();
      try {
        const result = await withTimeout(service.exit, STOP_TIMEOUT_MS, "OpenCode owner final shutdown");
        const stopReceipt = await readJson(service.stopPath);
        if (result.code !== 0 || result.signal !== null || stopReceipt?.status !== "stopped" || stopReceipt?.process_exit_code !== result.code) {
          throw new Error("owned service shutdown was not confirmed");
        }
        spawnedServices.delete(service);
      } catch {
        process.exitCode = 1;
      }
    }
    if (smokePassed && spawnedServices.size === 0) {
      await rm(rootReal, { recursive: true, force: true });
    } else {
      console.error(`OpenCode selftest state is preserved for inspection at ${rootReal}`);
      process.exitCode = 1;
    }
  }
}

main().catch((error) => {
  console.error(`OpenCode persistence selftest: FAIL (${error instanceof Error ? error.message : "unknown error"})`);
  process.exitCode = 1;
});
