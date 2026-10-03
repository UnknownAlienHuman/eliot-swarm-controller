import { randomUUID, createHash } from "node:crypto";
import { open, lstat, mkdir, readFile, realpath, rename, unlink, writeFile } from "node:fs/promises";
import path from "node:path";

const SERVER_VERSION = "2.0.7";
const BUN_VERSION = "1.4.0";
const USERNAME = "opencode";
const READY_TIMEOUT_MS = 60_000;
const STOP_TIMEOUT_MS = 15_000;
const CLOSE_TIMEOUT_MS = 5_000;

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function pathKey(value) {
  return path.resolve(value).replace(/[\\/]+$/, "").toLocaleLowerCase("en-US");
}

function isInside(root, candidate) {
  const relative = path.relative(path.resolve(root), path.resolve(candidate));
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
}

function assertAbsolutePath(value, flag) {
  if (typeof value !== "string" || !path.isAbsolute(value)) {
    throw new Error(`${flag} must be an absolute path`);
  }
  return path.resolve(value);
}

function assertCanonicalUuid(value, flag) {
  if (typeof value !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)) {
    throw new Error(`${flag} must be a canonical UUID`);
  }
  return value;
}

async function assertPlainPath(target, kind) {
  const info = await lstat(target);
  if (info.isSymbolicLink() || (kind === "directory" ? !info.isDirectory() : !info.isFile())) {
    throw new Error(`Refusing non-regular ${kind} path`);
  }
  const actual = await realpath(target);
  if (pathKey(actual) !== pathKey(target)) {
    throw new Error(`Refusing a redirected ${kind} path`);
  }
}

async function ensureDirectory(target) {
  await mkdir(target, { recursive: true });
  await assertPlainPath(target, "directory");
}

async function assertContainedPlainFile(root, target, { required = true } = {}) {
  if (!isInside(root, target) || pathKey(root) === pathKey(target)) {
    throw new Error("Private files must be inside the explicit state root");
  }
  try {
    await assertPlainPath(target, "file");
  } catch (error) {
    if (!required && error?.code === "ENOENT") return false;
    throw error;
  }
  return true;
}

async function readJsonFile(target) {
  const data = await readFile(target, "utf8");
  try {
    return JSON.parse(data);
  } catch {
    throw new Error("Existing owner metadata is invalid; inspect it before continuing");
  }
}

async function writeExclusiveJson(target, value) {
  const handle = await open(target, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function writeJsonAtomically(target, value, nonce) {
  const temporary = `${target}.${process.pid}.${nonce}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`, "utf8");
    await handle.sync();
  } catch (error) {
    await handle.close().catch(() => {});
    await unlink(temporary).catch(() => {});
    throw error;
  }
  await handle.close();
  try {
    await rename(temporary, target);
  } catch (error) {
    await unlink(temporary).catch(() => {});
    throw error;
  }
}

async function readPasswordFile(root, passwordFile) {
  const passwordPath = assertAbsolutePath(passwordFile, "--password-file");
  await assertContainedPlainFile(root, passwordPath);
  const stat = await lstat(passwordPath);
  if (stat.size < 32 || stat.size > 4096) {
    throw new Error("Password file must contain between 32 and 4096 bytes");
  }
  const content = await readFile(passwordPath, "utf8");
  const password = content.replace(/[\r\n]+$/, "");
  if (password.length < 32 || password.includes("\n") || password.includes("\r") || /[^\x21-\x7e]/.test(password)) {
    throw new Error("Password file must contain one printable ASCII password of at least 32 characters");
  }
  return password;
}

async function prepareState(stateRoot, passwordFile) {
  const root = assertAbsolutePath(stateRoot, "--state-root");
  await assertPlainPath(root, "directory");
  const rootReal = await realpath(root);
  if (pathKey(rootReal) !== pathKey(root)) {
    throw new Error("State root must be a direct, non-reparse directory");
  }

  const dirs = Object.fromEntries(["data", "cache", "config", "state", "tmp", "home", "appdata", "localappdata", "workspace"].map((name) => [name, path.join(root, name)]));
  for (const dir of Object.values(dirs)) await ensureDirectory(dir);

  const configDir = path.join(dirs.config, "opencode");
  await ensureDirectory(configDir);
  const configFile = path.join(configDir, "opencode.json");
  if (!(await assertContainedPlainFile(root, configFile, { required: false }))) {
    await writeExclusiveJson(configFile, {});
  }

  const dbPath = path.join(dirs.data, "opencode.sqlite");
  await assertContainedPlainFile(root, dbPath, { required: false });
  const password = await readPasswordFile(root, passwordFile);
  const passwordPath = assertAbsolutePath(passwordFile, "--password-file");

  return { root, dirs, configDir, configFile, dbPath, password, passwordPath };
}

function applyPrivateEnvironment(state) {
  const allowed = new Set([
    "COMSPEC", "HOMEDRIVE", "HOMEPATH", "LANG", "LC_ALL", "NUMBER_OF_PROCESSORS", "OS",
    "PATH", "PATHEXT", "PROCESSOR_ARCHITECTURE", "PROCESSOR_IDENTIFIER", "PUBLIC", "SYSTEMDRIVE",
    "SYSTEMROOT", "TZ", "USERDOMAIN", "USERNAME", "WINDIR",
  ]);
  const originalPath = process.env.PATH ?? process.env.Path;
  for (const key of Object.keys(process.env)) {
    if (!allowed.has(key.toUpperCase())) delete process.env[key];
  }
  if (originalPath) process.env.PATH = originalPath;
  process.env.HOME = state.dirs.home;
  process.env.USERPROFILE = state.dirs.home;
  process.env.APPDATA = state.dirs.appdata;
  process.env.LOCALAPPDATA = state.dirs.localappdata;
  process.env.XDG_DATA_HOME = state.dirs.data;
  process.env.XDG_CACHE_HOME = state.dirs.cache;
  process.env.XDG_CONFIG_HOME = state.dirs.config;
  process.env.XDG_STATE_HOME = state.dirs.state;
  process.env.OPENCODE_CONFIG_DIR = state.configDir;
  process.env.OPENCODE_TEST_HOME = state.dirs.home;
  process.env.TMP = state.dirs.tmp;
  process.env.TEMP = state.dirs.tmp;
  process.env.TMPDIR = state.dirs.tmp;
}

async function loadNativeRuntime() {
  const [{ ServerProcess }, { LayerNode }, { Global }, { AppProcess }, { NodeServices }, { Cause, Effect, Exit, Fiber, Layer, ManagedRuntime }] = await Promise.all([
    import("@opencode/server/process"),
    import("@opencode/util/effect/layer-node"),
    import("@opencode/util/global"),
    import("@opencode/util/process"),
    import("@effect/platform-node"),
    import("effect"),
  ]);
  const layer = Layer.provideMerge(
    LayerNode.compile(LayerNode.group([Global.node, AppProcess.node])),
    NodeServices.layer,
  );
  return { ServerProcess, Cause, Effect, Exit, Fiber, ManagedRuntime, layer };
}

function authHeader(password) {
  return `Basic ${Buffer.from(`${USERNAME}:${password}`, "utf8").toString("base64")}`;
}

async function fetchWithDeadline(url, options, timeoutMs = 3000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal, redirect: "error" });
  } finally {
    clearTimeout(timer);
  }
}

async function waitForReady({ endpoint, password, pid, port, timeoutMs }) {
  const deadline = Date.now() + timeoutMs;
  let lastStatus = "not-listening";
  while (Date.now() < deadline) {
    try {
      const response = await fetchWithDeadline(`${endpoint}/api/info`, { headers: { authorization: authHeader(password) } });
      if (response.status === 200) {
        const info = await response.json();
        if (info.version !== SERVER_VERSION || info.pid !== pid || !Array.isArray(info.urls)) {
          throw new Error("Native ready response did not match the pinned server owner");
        }
        if (!info.urls.some((url) => isExactLoopbackOrigin(url, port))) {
          throw new Error("Native ready response did not report the owned loopback endpoint");
        }
        return info;
      }
      if (response.status !== 503) {
        throw new Error(`Native server health returned HTTP ${response.status}`);
      }
      lastStatus = "starting";
    } catch (error) {
      if (error instanceof Error && /did not match|did not report|returned HTTP/.test(error.message)) throw error;
      lastStatus = "not-ready";
    }
    await delay(100);
  }
  throw new Error(`Native OpenCode service did not become ready (${lastStatus})`);
}

export function isExactLoopbackOrigin(value, port) {
  if (typeof value !== "string" || !Number.isInteger(port) || port < 1 || port > 65535) return false;
  try {
    const parsed = new URL(value);
    const expectedAuthority = `http://127.0.0.1${port === 80 ? "" : `:${port}`}`;
    const rawAuthority = value.match(/^[a-z][a-z0-9+.-]*:\/\/[^/?#]*/i)?.[0];
    return parsed.origin === new URL(expectedAuthority).origin &&
      parsed.protocol === "http:" &&
      parsed.hostname === "127.0.0.1" &&
      parsed.username === "" &&
      parsed.password === "" &&
      rawAuthority === expectedAuthority;
  } catch {
    return false;
  }
}

export function validatePriorStopRecord(previous, receipt, { connectionPath, stopPath, databasePath }) {
  const validTime = (value) => typeof value === "string" && Number.isFinite(Date.parse(value));
  const validExitCode = (value) => Number.isInteger(value) && value >= 0 && value <= 255;
  const valid = previous && receipt &&
    previous.schema_version === 1 && previous.status === "stopped" &&
    typeof previous.owner_nonce === "string" && previous.owner_nonce.length > 0 &&
    Number.isSafeInteger(previous.pid) && previous.pid > 0 &&
    previous.runtime === "bun" && previous.runtime_version === BUN_VERSION &&
    previous.native_server === "@opencode/server" && previous.native_server_version === SERVER_VERSION &&
    previous.listener_closed === true && previous.runtime_disposed === true &&
    ["completed", "interrupted", "failed", "not_started"].includes(receipt.server_fiber) &&
    previous.server_fiber === receipt.server_fiber &&
    previous.process_exit_code === receipt.process_exit_code &&
    previous.connection_absent === true &&
    validTime(previous.stopped_at) && previous.stopped_at === receipt.completed_at &&
    typeof previous.connection_file === "string" && pathKey(previous.connection_file) === pathKey(connectionPath) &&
    typeof previous.stop_receipt === "string" && pathKey(previous.stop_receipt) === pathKey(stopPath) &&
    typeof previous.database_path === "string" && pathKey(previous.database_path) === pathKey(databasePath) &&
    receipt.schema_version === 1 && receipt.status === "stopped" &&
    receipt.owner_nonce === previous.owner_nonce && receipt.pid === previous.pid &&
    receipt.listener_closed === true && receipt.runtime_disposed === true &&
    receipt.connection_absent === true &&
    typeof receipt.reason === "string" && receipt.reason.length > 0 &&
    validTime(receipt.completed_at) && validExitCode(receipt.process_exit_code);
  if (!valid) throw new Error("Prior owner and stop receipt do not prove one matching clean shutdown");
}

function assertRecordedPidExited(pid) {
  if (!Number.isSafeInteger(pid) || pid < 1 || pid === process.pid) {
    throw new Error("Prior owner PID cannot be proven exited");
  }
  try {
    // Signal 0 is an existence probe; it never terminates the recorded process.
    process.kill(pid, 0);
  } catch (error) {
    if (error?.code === "ESRCH") return;
    throw new Error(`Prior owner PID exit is unconfirmed (${error?.code ?? "unknown error"})`);
  }
  throw new Error("Prior owner PID is still present; refusing to reuse its state directory");
}

async function waitForClosed(endpoint, timeoutMs = CLOSE_TIMEOUT_MS) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetchWithDeadline(`${endpoint}/api/info`, { headers: { connection: "close" } }, 500);
      await response.body?.cancel().catch(() => {});
    } catch (error) {
      if (error?.name === "AbortError") return false;
      const code = error?.cause?.code ?? error?.code;
      if (code === "ECONNREFUSED" || code === "ECONNRESET" || code === "EHOSTUNREACH") return true;
      if (/ECONNREFUSED|ECONNRESET|Unable to connect|fetch failed/i.test(String(error?.message))) return true;
    }
    await delay(100);
  }
  return false;
}

function classifyExit(Exit, Cause, exit) {
  if (Exit.isSuccess(exit)) return "completed";
  if (Cause.hasInterruptsOnly(exit.cause)) return "interrupted";
  return "failed";
}

async function bounded(promise, timeoutMs, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function connectionDigestMatches(connectionPath, expectedDigest, root) {
  if (!expectedDigest) return false;
  try {
    await assertContainedPlainFile(root, connectionPath);
    const bytes = await readFile(connectionPath);
    return createHash("sha256").update(bytes).digest("hex") === expectedDigest;
  } catch {
    return false;
  }
}

export async function startOwnedService({ stateRoot, passwordFile, port, readyTimeoutMs = READY_TIMEOUT_MS, modelCatalog = "refresh", ownerNonce, workspaceDirectory }) {
  const nonce = ownerNonce === undefined ? randomUUID() : assertCanonicalUuid(ownerNonce, "--owner-nonce");
  const launchWorkspace = workspaceDirectory === undefined
    ? undefined
    : assertAbsolutePath(workspaceDirectory, "--workspace-directory");
  if (launchWorkspace !== undefined) await assertPlainPath(launchWorkspace, "directory");
  if (process.versions.bun !== BUN_VERSION) {
    throw new Error(`OpenCode service owner requires Bun ${BUN_VERSION}`);
  }
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error("port must be an integer from 0 to 65535");
  if (modelCatalog !== "refresh" && modelCatalog !== "offline") {
    throw new Error("modelCatalog must be either refresh or offline");
  }
  const state = await prepareState(stateRoot, passwordFile);
  applyPrivateEnvironment(state);
  process.chdir(launchWorkspace ?? state.dirs.workspace);

  const ownerPath = path.join(state.root, "owner.json");
  const connectionPath = path.join(state.root, "connection.json");
  const stopPath = path.join(state.root, "stop-receipt.json");
  if (await assertContainedPlainFile(state.root, ownerPath, { required: false })) {
    const previous = await readJsonFile(ownerPath);
    if (await assertContainedPlainFile(state.root, connectionPath, { required: false })) {
      throw new Error("A connection record remains beside a stopped owner; inspect it before restart");
    }
    if (!(await assertContainedPlainFile(state.root, stopPath, { required: false }))) {
      throw new Error("A prior owner has no stop receipt; verify its process and state before restart");
    }
    const previousReceipt = await readJsonFile(stopPath);
    validatePriorStopRecord(previous, previousReceipt, {
      connectionPath,
      stopPath,
      databasePath: state.dbPath,
    });
    assertRecordedPidExited(previous.pid);
    await unlink(ownerPath);
  } else {
    if (await assertContainedPlainFile(state.root, connectionPath, { required: false })) {
      throw new Error("A connection record exists without a valid owner; inspect it before startup");
    }
    if (await assertContainedPlainFile(state.root, stopPath, { required: false })) {
      throw new Error("A stop receipt exists without its owner record; inspect it before startup");
    }
  }

  const startedAt = new Date().toISOString();
  const owner = {
    schema_version: 1,
    owner_nonce: nonce,
    status: "starting",
    runtime: "bun",
    runtime_version: BUN_VERSION,
    native_server: "@opencode/server",
    native_server_version: SERVER_VERSION,
    model_catalog_mode: modelCatalog,
    model_catalog_policy: modelCatalog === "offline" ? "no-snapshot-no-fetch" : "bundled-snapshot-public-refresh",
    pid: process.pid,
    started_at: startedAt,
    requested_port: port,
    state_root: state.root,
    database_path: state.dbPath,
    private_xdg: {
      data: state.dirs.data,
      cache: state.dirs.cache,
      config: state.dirs.config,
      state: state.dirs.state,
      tmp: state.dirs.tmp,
    },
    connection_file: connectionPath,
    stop_receipt: stopPath,
  };
  await writeExclusiveJson(ownerPath, owner);

  let runtime;
  let shutdownEffect;
  let serviceFiber;
  let completed;
  let boundAddress;
  let connectionDigest;
  let stopPromise;
  let runtimeDisposePromise;
  let runtimeDisposed = false;
  const bound = deferred();

  const cleanMetadata = async () => {
    try {
      if (!(await assertContainedPlainFile(state.root, connectionPath, { required: false }))) return true;
      if (!(await connectionDigestMatches(connectionPath, connectionDigest, state.root))) return false;
      await unlink(connectionPath);
      return true;
    } catch {
      return false;
    }
  };

  const disposeRuntime = async () => {
    if (!runtime) {
      runtimeDisposed = true;
      return true;
    }
    runtimeDisposePromise ??= Promise.resolve().then(() => runtime.dispose());
    try {
      await bounded(runtimeDisposePromise, STOP_TIMEOUT_MS, "OpenCode native runtime disposal");
      runtimeDisposed = true;
      return true;
    } catch {
      return false;
    }
  };

  const stop = (reason = "operator") => {
    if (stopPromise) return stopPromise;
    stopPromise = (async () => {
      const requestedAt = new Date().toISOString();
      const receipt = {
        schema_version: 1,
        owner_nonce: nonce,
        pid: process.pid,
        requested_at: requestedAt,
        reason,
        status: "requested",
      };
      await writeJsonAtomically(stopPath, receipt, nonce);
      owner.status = "stopping";
      owner.stop_requested_at = requestedAt;
      owner.stop_reason = reason;
      await writeJsonAtomically(ownerPath, owner, nonce);

      if (shutdownEffect) await runtime.runPromise(shutdownEffect);
      let exit = null;
      if (completed) {
        try {
          exit = await bounded(completed, STOP_TIMEOUT_MS, "OpenCode server shutdown");
        } catch {
          exit = null;
        }
      }
      const listenerClosed = boundAddress ? await waitForClosed(`http://127.0.0.1:${boundAddress.port}`) : true;
      const fiberResult = exit ? classifyExit(native.Exit, native.Cause, exit) : "unconfirmed";
      const serverShutdownConfirmed = listenerClosed && (fiberResult === "interrupted" || fiberResult === "completed" || (!boundAddress && fiberResult === "failed"));
      if (serverShutdownConfirmed) await disposeRuntime();
      const connectionAbsent = serverShutdownConfirmed && runtimeDisposed ? await cleanMetadata() : false;
      const shutdownConfirmed = serverShutdownConfirmed && runtimeDisposed && connectionAbsent;
      const completedAt = new Date().toISOString();
      receipt.completed_at = completedAt;
      receipt.listener_closed = listenerClosed;
      receipt.server_fiber = fiberResult;
      receipt.runtime_disposed = runtimeDisposed;
      receipt.connection_absent = connectionAbsent;
      receipt.status = shutdownConfirmed ? "stopped" : "unconfirmed";
      receipt.process_exit_code = shutdownConfirmed ? (fiberResult === "failed" ? 1 : 0) : null;
      await writeJsonAtomically(stopPath, receipt, nonce);

      if (shutdownConfirmed) {
        owner.status = "stopped";
        owner.stopped_at = completedAt;
        owner.server_fiber = fiberResult;
        owner.listener_closed = listenerClosed;
        owner.runtime_disposed = runtimeDisposed;
        owner.connection_absent = connectionAbsent;
        owner.process_exit_code = receipt.process_exit_code;
        await writeJsonAtomically(ownerPath, owner, nonce);
      } else {
        owner.status = "stop_unconfirmed";
        owner.stop_unconfirmed_at = completedAt;
        owner.server_fiber = fiberResult;
        owner.listener_closed = listenerClosed;
        owner.runtime_disposed = runtimeDisposed;
        owner.connection_absent = connectionAbsent;
        owner.process_exit_code = null;
        await writeJsonAtomically(ownerPath, owner, nonce);
      }
      return receipt;
    })();
    return stopPromise;
  };

  let native;
  try {
    native = await loadNativeRuntime();
    runtime = native.ManagedRuntime.make(native.layer);
    const lifecycle = {
      onListen(address, shutdown) {
        shutdownEffect = shutdown;
        return native.Effect.sync(() => {
          boundAddress = address;
          bound.resolve(address);
          return native.Effect.void;
        });
      },
    };
    const startEffect = native.Effect.scoped(native.Effect.gen(function* () {
      const server = yield* native.ServerProcess.start({
        hostname: "127.0.0.1",
        port,
        password: state.password,
        events: { persist: true },
        database: { path: state.dbPath },
        config: { directory: state.configDir, file: state.configFile, project: false },
        models: modelCatalog === "offline" ? { fetch: false, snapshot: false } : { fetch: true, snapshot: true },
        fs: { filewatcher: false },
        app: { name: "opencode", version: SERVER_VERSION, channel: "stable" },
      }, lifecycle);
      yield* server.shutdown;
    }));
    serviceFiber = runtime.runFork(startEffect);
    completed = runtime.runPromise(native.Fiber.await(serviceFiber));

    const first = await bounded(Promise.race([
      bound.promise.then((address) => ({ kind: "bound", address })),
      completed.then((exit) => ({ kind: "exit", exit })),
    ]), readyTimeoutMs, "OpenCode bind");
    if (first.kind !== "bound") {
      throw new Error(`OpenCode server exited before binding (${classifyExit(native.Exit, native.Cause, first.exit)})`);
    }
    if (first.address._tag !== "TcpAddress" || first.address.hostname !== "127.0.0.1" || (port !== 0 && first.address.port !== port)) {
      throw new Error("OpenCode server did not bind the exact configured loopback address");
    }

    const actualPort = first.address.port;
    owner.bound_port = actualPort;
    const endpoint = `http://127.0.0.1:${actualPort}`;
    const info = await waitForReady({ endpoint, password: state.password, pid: process.pid, port: actualPort, timeoutMs: readyTimeoutMs });
    const connection = {
      schema_version: 1,
      endpoint,
      pid: process.pid,
      username: USERNAME,
      password: state.password,
    };
    const connectionBytes = `${JSON.stringify(connection, null, 2)}\n`;
    connectionDigest = createHash("sha256").update(connectionBytes).digest("hex");
    await writeExclusiveJson(connectionPath, connection);
    owner.status = "ready";
    owner.endpoint = endpoint;
    owner.ready_at = new Date().toISOString();
    owner.native_reported_pid = info.pid;
    owner.connection_sha256 = connectionDigest;
    await writeJsonAtomically(ownerPath, owner, nonce);

    return {
      endpoint,
      pid: process.pid,
      databasePath: state.dbPath,
      ownerPath,
      connectionPath,
      stopPath,
      stop,
    };
  } catch (error) {
    if (runtime && !serviceFiber) {
      const disposed = await disposeRuntime();
      const failedAt = new Date().toISOString();
      owner.status = disposed ? "stopped" : "stop_unconfirmed";
      owner.server_fiber = "not_started";
      owner.listener_closed = true;
      owner.runtime_disposed = disposed;
      owner.connection_absent = true;
      owner[disposed ? "stopped_at" : "stop_unconfirmed_at"] = failedAt;
      owner.process_exit_code = disposed ? 1 : null;
      await writeJsonAtomically(ownerPath, owner, nonce).catch(() => {});
      await writeJsonAtomically(stopPath, {
        schema_version: 1,
        owner_nonce: nonce,
        pid: process.pid,
        completed_at: failedAt,
        reason: "startup-failure-before-server-fiber",
        status: disposed ? "stopped" : "unconfirmed",
        server_fiber: "not_started",
        listener_closed: true,
        runtime_disposed: disposed,
        connection_absent: true,
        process_exit_code: disposed ? 1 : null,
      }, nonce).catch(() => {});
    } else if (runtime) {
      try {
        await stop("startup-failure");
      } catch {
        // Keep any unconfirmed owner marker and connection for operator inspection.
      }
    } else {
      owner.status = "stopped";
      owner.failed_at = new Date().toISOString();
      owner.stopped_at = owner.failed_at;
      owner.server_fiber = "not_started";
      owner.listener_closed = true;
      owner.runtime_disposed = true;
      owner.connection_absent = true;
      owner.process_exit_code = 1;
      await writeJsonAtomically(ownerPath, owner, nonce).catch(() => {});
      await writeJsonAtomically(stopPath, {
        schema_version: 1,
        owner_nonce: nonce,
        pid: process.pid,
        completed_at: owner.failed_at,
        reason: "startup-failure-before-runtime",
        status: "stopped",
        server_fiber: "not_started",
        listener_closed: true,
        runtime_disposed: true,
        connection_absent: true,
        process_exit_code: 1,
      }, nonce).catch(() => {});
    }
    throw error;
  }
}

function parseArgs(argv) {
  const args = new Map();
  let stopOnStdinEof = false;
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help" && args.size === 0) return { help: true };
    if (arg === "--stop-on-stdin-eof") {
      stopOnStdinEof = true;
      continue;
    }
    if (!arg.startsWith("--") || args.has(arg)) throw new Error(`Invalid or duplicate argument: ${arg}`);
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) throw new Error(`Missing value for ${arg}`);
    args.set(arg, value);
    index += 1;
  }
  if (args.has("--help")) return { help: true };
  const expected = new Set(["--state-root", "--password-file", "--port", "--ready-timeout-ms", "--model-catalog", "--owner-nonce", "--workspace-directory"]);
  for (const key of args.keys()) if (!expected.has(key)) throw new Error(`Unsupported argument: ${key}`);
  for (const key of ["--state-root", "--password-file", "--port"]) if (!args.has(key)) throw new Error("Required arguments: --state-root, --password-file, --port");
  const port = Number(args.get("--port"));
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error("--port must be an integer from 0 to 65535");
  const ownerNonce = args.has("--owner-nonce") ? assertCanonicalUuid(args.get("--owner-nonce"), "--owner-nonce") : undefined;
  const workspaceDirectory = args.has("--workspace-directory")
    ? assertAbsolutePath(args.get("--workspace-directory"), "--workspace-directory")
    : undefined;
  const readyTimeoutMs = args.has("--ready-timeout-ms") ? Number(args.get("--ready-timeout-ms")) : READY_TIMEOUT_MS;
  if (!Number.isInteger(readyTimeoutMs) || readyTimeoutMs < 1000 || readyTimeoutMs > 300_000) throw new Error("--ready-timeout-ms must be an integer from 1000 to 300000");
  const modelCatalog = args.get("--model-catalog") ?? "refresh";
  if (modelCatalog !== "refresh" && modelCatalog !== "offline") throw new Error("--model-catalog must be refresh or offline");
  return { stateRoot: args.get("--state-root"), passwordFile: args.get("--password-file"), port, readyTimeoutMs, modelCatalog, ownerNonce, workspaceDirectory, stopOnStdinEof };
}

async function runCli(argv) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log("Usage: bun serve.mjs --state-root <absolute-private-dir> --password-file <absolute-private-file> --port <0..65535> [--model-catalog <refresh|offline>] [--ready-timeout-ms <1000..300000>] [--owner-nonce <uuid>] [--workspace-directory <absolute-existing-directory>] [--stop-on-stdin-eof]");
    return;
  }
  const service = await startOwnedService(options);
  console.log(`[opencode-owner] ready version=${SERVER_VERSION} endpoint=${service.endpoint} pid=${service.pid}`);
  console.log(`[opencode-owner] owner=${service.ownerPath} connection=${service.connectionPath} database=${service.databasePath}`);

  await new Promise((resolve) => {
    let stopping = false;
    const requestStop = (reason) => {
      if (stopping) return;
      stopping = true;
      void service.stop(reason).then((receipt) => {
        console.log(`[opencode-owner] stop=${receipt.status} fiber=${receipt.server_fiber ?? "unconfirmed"}`);
        if (receipt.status !== "stopped") process.exitCode = 1;
        resolve();
      }).catch(() => {
        console.error("[opencode-owner] stop remains unconfirmed; inspect the private owner and stop receipt");
        process.exitCode = 1;
        resolve();
      });
    };
    const onSigint = () => requestStop("SIGINT");
    const onSigterm = () => requestStop("SIGTERM");
    const onStdinEnd = () => requestStop("stdin-eof");
    process.once("SIGINT", onSigint);
    process.once("SIGTERM", onSigterm);
    if (options.stopOnStdinEof) {
      process.stdin.once("end", onStdinEnd);
      process.stdin.resume();
    }
  });
}

if (import.meta.main) {
  runCli(process.argv.slice(2)).catch((error) => {
    console.error(`[opencode-owner] failed: ${error instanceof Error ? error.message : "unknown startup error"}`);
    process.exitCode = 1;
  });
}
