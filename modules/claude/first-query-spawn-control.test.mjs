 // Test-composition only. It holds the real Store-owned first TaskPrompt until
 // the exact SDK ChildProcess has exited, then gives the unchanged message to
 // the installed SDK's AsyncIterable writer. It never manufactures SDK frames.
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import {
  lstat, readFile, readdir, rename, writeFile,
} from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const CONTROL_ENV = 'ELIOT_CLAUDE_V5_FIRST_QUERY_FAULT_DIR';

function fail(code) { throw new Error(code); }

function exactFields(value, fields) {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join('\0') === [...fields].sort().join('\0');
}

function sha256(text) {
  return createHash('sha256').update(text, 'utf8').digest('hex');
}

export async function createFirstQueryFaultControl({ bindingId, generation, bootId }) {
  const configured = process.env[CONTROL_ENV];
  if (typeof configured !== 'string' || !path.isAbsolute(configured)) {
    fail('TEST_FAULT_CONTROL_DIRECTORY_REQUIRED');
  }
  const directory = path.resolve(configured);
  const directoryStat = await lstat(directory).catch(() => null);
  if (!directoryStat?.isDirectory() || directoryStat.isSymbolicLink()) {
    fail('TEST_FAULT_CONTROL_DIRECTORY_INVALID');
  }
  if ((await readdir(directory)).length !== 0) fail('TEST_FAULT_CONTROL_DIRECTORY_NOT_EMPTY');

  const state = {
    child: null,
    childId: null,
    spawned: null,
    spawnRecordPromise: null,
    exitPromise: null,
    exitRecord: null,
    faultContext: null,
    faultRequested: false,
    sdkInitRoot: null,
    helloSequence: 0,
    unknownOutcomeAttempts: 0,
    failure: null,
  };

  async function writeRecord(name, record) {
    const temporary = path.join(directory, name + '.' + randomUUID() + '.tmp');
    const destination = path.join(directory, name);
    await writeFile(temporary, JSON.stringify(record) + '\n', { flag: 'wx' });
    await rename(temporary, destination);
  }

  function spawnClaudeCodeProcess(spawnOptions) {
    if (state.child) fail('TEST_FAULT_CONTROL_MULTIPLE_SDK_CHILDREN');
    if (typeof spawnOptions?.command !== 'string' || !Array.isArray(spawnOptions.args)
        || !spawnOptions.env || typeof spawnOptions.env !== 'object') {
      fail('TEST_FAULT_CONTROL_SDK_SPAWN_OPTIONS_INVALID');
    }

    // These are the SDK 0.3.287 spawnLocalProcess settings. Preserve its exact
    // command/args/cwd/env/signal values and return Node's actual child object.
    const child = spawn(spawnOptions.command, spawnOptions.args, {
      cwd: spawnOptions.cwd,
      env: spawnOptions.env,
      signal: spawnOptions.signal,
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });
    const childId = randomUUID();
    state.child = child;
    state.childId = childId;

    let resolveSpawnRecord;
    let rejectSpawnRecord;
    state.spawnRecordPromise = new Promise((resolve, reject) => {
      resolveSpawnRecord = resolve;
      rejectSpawnRecord = reject;
    });
    state.spawnRecordPromise.catch(() => {});
    child.once('spawn', () => {
      state.spawned = {
        schema_version: 1,
        event: 'sdk_child_spawned',
        child_id: childId,
        pid: child.pid,
        binding_id: bindingId,
        generation,
        bridge_boot_id: bootId,
      };
      void writeRecord('sdk-child.json', state.spawned)
        .then(resolveSpawnRecord, error => {
          state.failure = error;
          rejectSpawnRecord(error);
        });
    });
    child.once('error', error => {
      state.failure = error;
      rejectSpawnRecord(error);
    });

    let resolveExit;
    state.exitPromise = new Promise(resolve => { resolveExit = resolve; });
    child.once('close', (exitCode, signal) => {
      const faultContext = state.faultContext;
      const record = {
        schema_version: 1,
        event: 'sdk_child_closed',
        child_id: childId,
        pid: child.pid,
        binding_id: bindingId,
        generation,
        bridge_boot_id: bootId,
        ...(faultContext ? {
          operation_id: faultContext.operationId,
          input_id: faultContext.inputId,
        } : {}),
        exit_code: exitCode,
        signal,
        fault_requested: state.faultRequested,
      };
      state.exitRecord = record;
      void writeRecord('sdk-child-exit.json', record)
        .then(() => resolveExit(record), error => {
          state.failure = error;
          resolveExit(null);
        });
    });

    return child;
  }

  async function noteSdkInitIdentity(rootId) {
    if (typeof rootId !== 'string' || !rootId.trim()) fail('TEST_FAULT_CONTROL_INIT_IDENTITY_INVALID');
    if (state.sdkInitRoot && state.sdkInitRoot !== rootId) fail('TEST_FAULT_CONTROL_INIT_IDENTITY_CHANGED');
    if (!state.sdkInitRoot) {
      state.sdkInitRoot = rootId;
      await writeRecord('sdk-init-root.json', {
        schema_version: 1,
        event: 'sdk_init_session_id_observed',
        binding_id: bindingId,
        generation,
        bridge_boot_id: bootId,
        sdk_observed_root_id: rootId,
      });
    }
  }

  async function readOutcomeAckRelease() {
    const file = path.join(directory, 'release-first-query-outcome-ack.json');
    for (;;) {
      if (state.failure) fail('TEST_FAULT_CONTROL_CHILD_EVENT_FAILED');
      try {
        return JSON.parse(await readFile(file, 'utf8'));
      } catch (error) {
        if (error.code !== 'ENOENT') fail('TEST_FAULT_CONTROL_ACK_RELEASE_INVALID');
      }
      await delay(20);
    }
  }

  // Keep the actual Control call and Store response; this test-only wrapper withholds the first response and holds the retry.
  function instrumentControl(control) {
    const originalCall = control.call.bind(control);
    control.call = async (method, params) => {
      const response = await originalCall(method, params);
      if (method === 'module.hello') {
        const sequence = ++state.helloSequence;
        if (sequence > 2) fail('TEST_FAULT_CONTROL_UNEXPECTED_HELLO_COUNT');
        await writeRecord('module-hello-' + sequence + '.json', {
          schema_version: 1,
          event: 'real_store_module_hello',
          sequence,
          binding_id: response?.binding_id ?? null,
          generation: response?.generation ?? null,
          request_native_root_id: params?.native_root_id ?? null,
          request_native_scope_key: params?.native_scope_key ?? null,
          store_native_root_id: response?.native_root_id ?? null,
          store_native_scope_key: response?.native_scope_key ?? null,
        });
      }
      if (method === 'module.outcome'
          && params?.details?.diagnostic_code === 'FIRST_NATIVE_QUERY_UNKNOWN') {
        const attempt = ++state.unknownOutcomeAttempts;
        if (attempt > 2) fail('TEST_FAULT_CONTROL_UNEXPECTED_UNKNOWN_RETRY_COUNT');
        if (response?.recorded !== true) fail('TEST_FAULT_CONTROL_STORE_OUTCOME_NOT_RECORDED');
        const receipt = params?.details?.module_receipt;
        const record = {
          schema_version: 1,
          event: 'real_store_unknown_outcome_response_observed',
          attempt,
          operation_id: params?.operation_id ?? null,
          outcome: params?.outcome ?? null,
          diagnostic_code: params?.details?.diagnostic_code ?? null,
          binding_id: receipt?.binding_id ?? null,
          generation: receipt?.binding_generation ?? null,
          bridge_boot_id: params?.details?.bridge_boot_id ?? null,
          native_root_id: params?.native_root_id ?? null,
          native_scope_key: params?.native_scope_key ?? null,
          request_sha256: sha256(JSON.stringify(params)),
          store_recorded: response.recorded,
        };
        await writeRecord('first-query-outcome-' + attempt + '.json', record);
        if (attempt === 1) {
          const error = new Error('TEST_FIRST_QUERY_OUTCOME_ACK_WITHHELD');
          error.code = 'TEST_FIRST_QUERY_OUTCOME_ACK_WITHHELD';
          throw error;
        }
        const release = await readOutcomeAckRelease();
        if (!exactFields(release, [
          'schema_version', 'binding_id', 'generation', 'bridge_boot_id', 'operation_id', 'attempt',
        ]) || release.schema_version !== 1
            || release.binding_id !== bindingId || release.generation !== generation
            || release.bridge_boot_id !== bootId || release.operation_id !== params.operation_id
            || release.attempt !== 2) {
          fail('TEST_FAULT_CONTROL_ACK_RELEASE_IDENTITY_MISMATCH');
        }
        await writeRecord('first-query-outcome-ack-released.json', {
          schema_version: 1,
          event: 'real_store_unknown_outcome_ack_released_to_bridge',
          attempt,
          operation_id: params.operation_id,
          binding_id: bindingId,
          generation,
          bridge_boot_id: bootId,
          store_recorded: response.recorded,
        });
      }
      return response;
    };
  }

  async function readFaultRequest() {
    const file = path.join(directory, 'fault-request.json');
    for (;;) {
      if (state.failure) fail('TEST_FAULT_CONTROL_CHILD_EVENT_FAILED');
      try {
        return JSON.parse(await readFile(file, 'utf8'));
      } catch (error) {
        if (error.code !== 'ENOENT') fail('TEST_FAULT_CONTROL_REQUEST_INVALID');
      }
      await delay(20);
    }
  }

  async function* gateFirstTaskPrompt(iterable, { operationId, inputId, taskPrompt }) {
    let gated = false;
    for await (const message of iterable) {
      if (!gated) {
        gated = true;
        if (!state.child || !state.spawnRecordPromise || !state.exitPromise
            || typeof operationId !== 'string' || typeof inputId !== 'string'
            || taskPrompt?.context?.operation_id !== operationId
            || taskPrompt?.context?.worker_boot_id !== bootId
            || message?.type !== 'user' || message.uuid !== inputId
            || message.parent_tool_use_id !== null || Object.hasOwn(message, 'session_id')
            || message.message?.role !== 'user'
            || message.message.content !== taskPrompt.envelope?.prompt
            || sha256(message.message.content) !== taskPrompt.envelope?.prompt_sha256
            || Buffer.byteLength(message.message.content, 'utf8') !== taskPrompt.envelope?.prompt_bytes) {
          fail('TEST_FAULT_CONTROL_TASKPROMPT_IDENTITY_MISMATCH');
        }
        await state.spawnRecordPromise;
        const pending = {
          schema_version: 1,
          event: 'store_taskprompt_pending_before_sdk_write',
          child_id: state.childId,
          pid: state.child.pid,
          binding_id: bindingId,
          generation,
          bridge_boot_id: bootId,
          operation_id: operationId,
          input_id: inputId,
          task_id: taskPrompt.context.task_id,
          attempt_id: taskPrompt.context.attempt_id,
          task_snapshot_sha256: taskPrompt.context.task_snapshot_sha256,
          prompt_sha256: taskPrompt.envelope.prompt_sha256,
        };
        await writeRecord('pending-taskprompt.json', pending);

        const request = await readFaultRequest();
        if (!exactFields(request, [
          'schema_version', 'child_id', 'pid', 'binding_id', 'generation',
          'bridge_boot_id', 'operation_id', 'input_id',
        ]) || request.schema_version !== 1
            || request.child_id !== state.childId || request.pid !== state.child.pid
            || request.binding_id !== bindingId || request.generation !== generation
            || request.bridge_boot_id !== bootId || request.operation_id !== operationId
            || request.input_id !== inputId) {
          fail('TEST_FAULT_CONTROL_REQUEST_IDENTITY_MISMATCH');
        }

        const child = state.child;
        if (child.exitCode !== null || child.signalCode != null || child.killed) {
          fail('TEST_FAULT_CONTROL_CHILD_NOT_LIVE_AT_REQUEST');
        }
        state.faultContext = { operationId, inputId };
        if (!child.kill('SIGTERM')) fail('TEST_FAULT_CONTROL_EXACT_CHILD_KILL_REJECTED');
        state.faultRequested = true;

        // The barrier is the actual ChildProcess close event, after the SDK's
        // exit listeners have seen the same child. No PID-based kill or proxy.
        const exited = await state.exitPromise;
        if (!exited || !exited.fault_requested || exited.child_id !== state.childId
            || exited.pid !== child.pid || exited.operation_id !== operationId
            || exited.input_id !== inputId
            || (exited.exit_code === null && exited.signal === null)) {
          fail('TEST_FAULT_CONTROL_REAL_EXIT_BARRIER_MISSING');
        }
      }
      yield message;
    }
  }

  return {
    spawnClaudeCodeProcess,
    gateFirstTaskPrompt,
    noteSdkInitIdentity,
    instrumentControl,
  };
}
