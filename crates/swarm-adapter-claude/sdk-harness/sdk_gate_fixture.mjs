// Gate fixture for the Claude SDK runtime capability admission.
//
// Ownership: this file plus sdk-harness/bridge.mjs only. It exercises the
// package-admission seam of prepare() without a live SDK: it builds a fake
// @anthropic-ai/claude-agent-sdk package tree under a temp sdk_runtime_root
// and asserts the gate's observable contract for four cases.
//
// Case A (compatible different version): the package identity is correct and
// the module exports startup returning a WarmQuery (query + close). The gate
// must admit it and report the observed version in the prepared frame.
// Case B (missing startup export): the package identity is correct but the
// module exports no startup function. The gate must fail with
// SDK_STARTUP_UNAVAILABLE.
// Case C (wrong package identity): the package name differs. The gate must
// fail with SDK_PACKAGE_IDENTITY_MISMATCH.
//
// No dependency or version upgrade, no invented SDK exports: the fake module
// only implements the startup/WarmQuery interface the bridge already uses.
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export async function buildFakeSdk({ version, name = '@anthropic-ai/claude-agent-sdk', startupBody = null }) {
  const runtimeRoot = await mkdtemp(path.join(tmpdir(), 'claude-sdk-gate-'));
  const packageDirectory = path.join(runtimeRoot, 'node_modules', '@anthropic-ai', 'claude-agent-sdk');
  await mkdir(packageDirectory, { recursive: true });
  const moduleSource = startupBody === null
    ? 'export async function startup() { return { query() {}, close() {} }; }'
    : startupBody;
  await writeFile(path.join(packageDirectory, 'package.json'), JSON.stringify({
    name,
    version,
    type: 'module',
    main: './sdk.mjs',
  }));
  await writeFile(path.join(packageDirectory, 'sdk.mjs'), moduleSource);
  return runtimeRoot;
}

export const CASES = [
  {
    name: 'compatible different version is admitted',
    package: { version: '0.3.288' },
    expectDiagnostic: null,
  },
  {
    name: 'missing startup export fails startup check',
    package: { version: '0.3.288', startupBody: 'export const notStartup = 1;' },
    expectDiagnostic: 'SDK_STARTUP_UNAVAILABLE',
  },
  {
    name: 'warm query without close fails handle check',
    package: {
      version: '0.3.288',
      startupBody: 'export async function startup() { return { query() {} }; }',
    },
    expectDiagnostic: 'SDK_STARTUP_UNKNOWN',
  },
  {
    name: 'wrong package identity fails identity check',
    package: { version: '0.3.288', name: '@anthropic-ai/other-sdk' },
    expectDiagnostic: 'SDK_PACKAGE_IDENTITY_MISMATCH',
  },
];

export async function cleanup(runtimeRoot) {
  await rm(runtimeRoot, { recursive: true, force: true });
}
const BRIDGE_TIMEOUT_MS = 10_000;

// One bounded real bridge lifetime. Frame buffering crosses actual pipe chunk
// boundaries; close is observed after both output pipes have drained.
async function runPrepare({ runtimeRoot, operationId }) {
  const bridgePath = fileURLToPath(new URL('./bridge.mjs', import.meta.url));
  const child = spawn(process.execPath, [bridgePath], { stdio: ['pipe', 'pipe', 'pipe'] });
  const command = {
    kind: 'prepare', operation_id: operationId,
    workspace_root: runtimeRoot, sdk_runtime_root: runtimeRoot,
    bridge_boot_id: 'gate-fixture-boot', native_scope_key: 'gate-fixture-scope',
    binding_id: 'gate-fixture-binding', generation: 1,
    model_id: 'gate-fixture-model', permission_mode: null,
    allow_dangerously_skip_permissions: false, native_executable: null,
  };
  return await new Promise(resolve => {
    let response = null;
    let stdout = '';
    let stderr = '';
    let stopped = false;
    let spawnError = null;
    let timedOut = false;
    let killTimer;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
      killTimer = setTimeout(() => resolve({exitCode: null, frame: response, stopped,
        timedOut: true, stderr, spawnError, closed: false}), 2_000);
    }, BRIDGE_TIMEOUT_MS);
    child.once('error', error => { spawnError = String(error); });
    child.stdin.on('error', error => { spawnError ??= String(error); });
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-8192); });
    child.stdout.on('data', chunk => {
      stdout += chunk;
      let end;
      while ((end = stdout.indexOf('\n')) !== -1) {
        const line = stdout.slice(0, end);
        stdout = stdout.slice(end + 1);
        if (!line) continue;
        let frame;
        try { frame = JSON.parse(line); }
        catch { spawnError = 'malformed bridge frame'; continue; }
        if (frame.kind === 'stopped') stopped = true;
        if (!response && ['prepared', 'operation_rejected', 'operation_unknown', 'diagnostic'].includes(frame.kind)) {
          response = frame;
          if (frame.kind !== 'operation_unknown') child.stdin.write(JSON.stringify({kind: 'stop'}) + '\n');
          child.stdin.end();
        }
      }
    });
    child.once('close', exitCode => {
      clearTimeout(timer);
      clearTimeout(killTimer);
      resolve({exitCode, frame: response, stopped, stderr, spawnError, timedOut, closed: true});
    });
    child.stdin.write(JSON.stringify(command) + '\n');
  });
}
export async function runGateFixture() {
  const results = [];
  for (const testCase of CASES) {
    const runtimeRoot = await buildFakeSdk(testCase.package);
    try {
      const { exitCode, frame, stopped, stderr, spawnError, timedOut, closed } = await runPrepare({
        runtimeRoot,
        operationId: 'gate-fixture-operation',
      });
      const actual = frame?.kind === 'prepared'
        ? null
        : frame?.diagnostic_code ?? frame?.code ?? frame?.kind ?? null;
      // A prepared frame must carry the observed SDK version equal to the
      // fake package version; a rejection must carry the expected diagnostic.
      const versionOk = frame?.kind === 'prepared'
        ? frame.sdk_version === testCase.package.version
        : true;
      // A clean stop without an admission frame is not successful SDK prepare.
      const admissionObserved = testCase.expectDiagnostic !== null || frame?.kind === 'prepared';
      results.push({
        case: testCase.name,
        expected: testCase.expectDiagnostic,
        actual,
        observedVersion: frame?.sdk_version ?? null,
        versionOk,
        stopped,
        exitCode,
        frameKind: frame?.kind,
        stderr: stderr || null,
        spawnError,
        pass: admissionObserved && actual === testCase.expectDiagnostic && versionOk && exitCode === 0 && !spawnError && !timedOut && closed && (frame?.kind === 'operation_unknown' || stopped),
      });
    } finally {
      await cleanup(runtimeRoot);
    }
  }
  return results;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const results = await runGateFixture();
  for (const result of results) {
    console.log(JSON.stringify(result));
  }
  const failed = results.filter(result => !result.pass);
  process.exit(failed.length === 0 ? 0 : 1);
}
