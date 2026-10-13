// The descriptor's module_host_config_path is HostConnectionConfig schema 1.
// Credentials, owner and selected descriptor identity come from the existing
// module-owner environment, never from this transport-only file.
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import {
  CLAUDE_TASK_PROMPT_ARTIFACT_ID, CLAUDE_TASK_PROMPT_ARTIFACT_VERSION,
} from './task-prompt.mjs';

function fail(code) { throw new Error(code); }
function record(value) { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function fields(value, required, optional = []) {
  return record(value) && required.every(key => Object.hasOwn(value, key))
    && Object.keys(value).every(key => required.includes(key) || optional.includes(key));
}
function required(env, key) {
  const value = env[key];
  if (typeof value !== 'string' || !value.trim() || Buffer.byteLength(value, 'utf8') > 64 * 1024) {
    fail('MODULE_BOOTSTRAP_VALUE_REQUIRED');
  }
  return value;
}
async function readJson(file) {
  const bytes = await readFile(file);
  if (bytes.length > 64 * 1024) fail('MODULE_BOOTSTRAP_FILE_TOO_LARGE');
  try { return JSON.parse(bytes.toString('utf8')); }
  catch { fail('MODULE_BOOTSTRAP_JSON_INVALID'); }
}

// Same persisted discovery convention as swarm-client::ipc_endpoint.
export async function hostEndpoint(root) {
  if (typeof root !== 'string' || !path.isAbsolute(root)) fail('MODULE_HOST_CONFIG_INVALID');
  let canonical = await realpath(root);
  if (process.platform === 'win32') {
    // std::fs::canonicalize uses the Windows extended-length namespace.
    if (!canonical.startsWith('\\\\?\\')) {
      canonical = canonical.startsWith('\\\\')
        ? '\\\\?\\UNC\\' + canonical.slice(2)
        : '\\\\?\\' + canonical;
    }
    const suffix = createHash('sha256').update(canonical.toLowerCase(), 'utf8').digest('hex').slice(0, 32);
    return '\\\\.\\pipe\\eliot-swarm-' + suffix;
  }
  const endpoint = path.join(canonical, 'control.sock');
  if (Buffer.byteLength(endpoint, 'utf8') > 100) fail('MODULE_HOST_CONFIG_INVALID');
  return endpoint;
}

function schemaList(actual, expected) {
  return Array.isArray(actual) && actual.length === expected.length
    && actual.every((schema, index) => fields(schema, ['schema_id', 'version'], ['sha256'])
      && schema.schema_id === expected[index] && schema.version === '1'
      && (schema.sha256 == null || (typeof schema.sha256 === 'string' && /^[0-9a-f]{64}$/.test(schema.sha256))));
}

export async function loadModuleBootstrap(configFile, env = process.env) {
  const host = await readJson(configFile);
  if (!fields(host, ['schema_version', 'host_data_dir'], ['ipc']) || host.schema_version !== 1) {
    fail('MODULE_HOST_CONFIG_INVALID');
  }
  const ipc = host.ipc ?? {};
  const ipcKeys = ['max_frame_bytes', 'max_connections', 'max_inflight_per_connection', 'write_timeout_seconds'];
  if (!fields(ipc, [], ipcKeys) || Object.values(ipc).some(value => !Number.isSafeInteger(value) || value < 0)) {
    fail('MODULE_HOST_CONFIG_INVALID');
  }
  const endpoint = await hostEndpoint(host.host_data_dir);
  const stateDir = required(env, 'ELIOT_SWARM_MODULE_STATE');
  const ownerFile = required(env, 'ELIOT_SWARM_MODULE_OWNER');
  const credentialFile = required(env, 'ELIOT_SWARM_MODULE_CREDENTIAL_FILE');
  if (!path.isAbsolute(stateDir) || ownerFile !== path.join(stateDir, 'owner.json')
      || !path.isAbsolute(credentialFile)) fail('INVALID_MODULE_OWNER_PATH');
  const managedOwner = await readJson(ownerFile);
  if (managedOwner.version !== 1 || managedOwner.process?.purpose !== 'module'
      || typeof managedOwner.token !== 'string' || !managedOwner.token.trim()) fail('INVALID_MODULE_OWNER_RECORD');
  const credential = await readJson(credentialFile);
  const clientId = required(env, 'ELIOT_SWARM_MODULE_CLIENT_ID');
  if (!fields(credential, ['client_id', 'token']) || credential.client_id !== clientId
      || typeof credential.token !== 'string' || !credential.token.trim()) fail('MODULE_CREDENTIAL_MISMATCH');
  const bindingId = required(env, 'ELIOT_SWARM_MODULE_BINDING_ID');
  const generationText = required(env, 'ELIOT_SWARM_MODULE_BINDING_GENERATION');
  const generation = Number(generationText);
  if (!/^[1-9][0-9]*$/.test(generationText) || !Number.isSafeInteger(generation)) fail('MODULE_BINDING_IDENTITY');
  const bootId = required(env, 'ELIOT_SWARM_MODULE_BOOT_ID');
  let moduleContract;
  try { moduleContract = JSON.parse(required(env, 'ELIOT_SWARM_MODULE_CONTRACT')); }
  catch { fail('MODULE_CONTRACT_INVALID'); }
  const buildId = env.ELIOT_SWARM_MODULE_BUILD_ID ?? null;
  if (!fields(moduleContract, [
    'schema_version', 'module_id', 'artifact', 'protocol', 'capabilities',
    'config_schema', 'command_schemas', 'event_schemas',
  ], ['pre_input_open']) || moduleContract.schema_version !== 1
      || moduleContract.module_id !== 'claude'
      || required(env, 'ELIOT_SWARM_MODULE_ID') !== moduleContract.module_id
      || !fields(moduleContract.artifact, ['artifact_id', 'version'], ['build_id'])
      || moduleContract.artifact.artifact_id !== CLAUDE_TASK_PROMPT_ARTIFACT_ID
      || moduleContract.artifact.version !== CLAUDE_TASK_PROMPT_ARTIFACT_VERSION
      || required(env, 'ELIOT_SWARM_MODULE_ARTIFACT_ID') !== moduleContract.artifact.artifact_id
      || required(env, 'ELIOT_SWARM_MODULE_ARTIFACT_VERSION') !== moduleContract.artifact.version
      || (moduleContract.artifact.build_id ?? null) !== buildId
      || (buildId !== null && (typeof buildId !== 'string' || !buildId.trim()))
      || !isDeepStrictEqual(moduleContract.protocol, { major: 1, minor: 0 })
      || required(env, 'ELIOT_SWARM_MODULE_PROTOCOL') !== '1.0'
      || moduleContract.config_schema !== null
      || !isDeepStrictEqual(moduleContract.capabilities, [
        'agent.open', 'agent.reconcile', 'agent.refresh', 'agent.send/next_turn', 'task.dispatch',
      ])
      || !schemaList(moduleContract.command_schemas, [
        'swarm.runtime_command', 'swarm.task_dispatch_context', 'swarm.task_prompt',
      ])
      || !schemaList(moduleContract.event_schemas, ['swarm.runtime_outcome', 'swarm.task_dispatch_admission'])) {
    fail('MODULE_CONTRACT_MISMATCH');
  }
  // Supervisor add_launch_value maps the literal COMMAND value to this name.
  // Native model/workspace/permission choices still come only from the route.
  const command = env.ELIOT_SWARM_CONFIG_COMMAND;
  return {
    config: { endpoint, moduleArtifactId: moduleContract.artifact.artifact_id, command },
    maxFrameBytes: ipc.max_frame_bytes ?? 1_048_576,
    credential, managedOwner, bootId, moduleContract, bindingId, generation,
  };
}

export function verifyModuleHello(hello, bootstrap) {
  const claim = bootstrap.moduleContract;
  const negotiation = hello?.module_contract_negotiation;
  if (hello?.binding_id !== bootstrap.bindingId || hello.generation !== bootstrap.generation
      || hello.route?.runtime !== 'module' || hello.route.module_artifact_id !== claim.artifact.artifact_id
      || negotiation?.status !== 'negotiated' || negotiation.source !== 'store_registered_descriptor'
      || !Number.isSafeInteger(negotiation.descriptor_revision) || negotiation.descriptor_revision <= 0
      || negotiation.effects_authorized_by_descriptor !== false
      || negotiation.module_id !== claim.module_id
      || ['artifact', 'protocol', 'capabilities', 'config_schema', 'command_schemas', 'event_schemas']
        .some(key => !isDeepStrictEqual(negotiation[key], claim[key]))
      || !isDeepStrictEqual(negotiation.pre_input_open ?? null, claim.pre_input_open ?? null)) {
    fail('MODULE_CONTRACT_NEGOTIATION_FAILED');
  }
}

export function verifyModuleCommand(command, bootstrap) {
  if (command?.binding_id !== bootstrap.bindingId || command?.generation !== bootstrap.generation
      || command.route?.runtime !== 'module'
      || command.route.module_artifact_id !== bootstrap.moduleContract.artifact.artifact_id) {
    fail('HOST_COMMAND_IDENTITY_INVALID');
  }
}
