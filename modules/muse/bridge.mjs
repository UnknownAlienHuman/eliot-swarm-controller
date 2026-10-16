#!/usr/bin/env node
// SDK-owned native execution. Host IPC reconnect never closes Muse or repeats input.
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import museDescriptor from './module-descriptor.template.json' with { type: 'json' };
import { spawnMspConnection, MspError } from '@muse-code/sdk';
import { Control } from './control.mjs';
import { configuration, modelMatches, retainModelOverride, goalCommand, nativeMcpServers, startNativeMcpSession } from './settings.mjs';
import { readResult } from './results.mjs';
import { spawnOwned } from './owned.mjs';
import { recoveryState } from './checkpoint.mjs';
import { commandIdFor, durabilityProfile, gapFillObservation, hostDeathObservation, failedReconcileOutcome } from './observe.mjs';
import {
  closedNativeUsageSnapshot,
  emptyNativeUsageSnapshot,
  nativeUsageReadSnapshot,
  nativeUsageSnapshot,
  restoredNativeUsageSnapshot,
} from './native-usage.mjs';

const MUSE_ARTIFACT_ID = 'muse-sdk-1.3.0-bridge.13';
const MUSE_ARTIFACT_VERSION = '13';
const MAX_TASK_PROMPT_BYTES = 1_000_000;
const MAX_OBSERVATION_STATE_BYTES = 700_000;
const SHA256 = /^[0-9a-f]{64}$/;
const TASK_PROMPT_FIELDS = [
  'schema_id', 'schema_version', 'task_id', 'task_revision', 'attempt_id',
  'task_snapshot_sha256', 'prompt_sha256', 'prompt_bytes', 'prompt',
];
const TASK_DISPATCH_CONTEXT_FIELDS = [
  'schema_version', 'operation_id', 'binding_id', 'binding_generation', 'worker_boot_id',
  'attempt_id', 'task_id', 'task_revision', 'task_snapshot_sha256', 'source_text_sha256',
  'source_text_bytes',
];

function required(object, key) {
  if (typeof object?.[key] !== 'string' || !object[key].trim()) throw new Error(`MISSING_${key}`);
  return object[key];
}
function isRecord(value) { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function exactFields(value, fields) {
  return isRecord(value) && Object.keys(value).length === fields.length && fields.every(key => Object.hasOwn(value,key));
}
function sha256(value) { return createHash('sha256').update(value).digest('hex'); }
function compareText(left,right) { return left<right?-1:left>right?1:0; }
function canonicalJson(value) {
  if(Array.isArray(value))return value.map(canonicalJson);
  if(isRecord(value))return Object.fromEntries(Object.keys(value).sort(compareText).map(key=>[key,canonicalJson(value[key])]));
  return value;
}
function sameJson(left,right) { return JSON.stringify(canonicalJson(left))===JSON.stringify(canonicalJson(right)); }
function claimSchema(schema) {
  if(!isRecord(schema) || typeof schema.schema_id!=='string' || typeof schema.version!=='string')
    throw new Error('MODULE_DESCRIPTOR_SCHEMA_INVALID');
  return {schema_id:schema.schema_id,version:schema.version,...(schema.sha256==null?{}:{sha256:schema.sha256})};
}
function moduleContractClaim(descriptor) {
  const artifact=descriptor?.artifact;
  const protocol=descriptor?.protocol;
  if(!isRecord(descriptor) || descriptor.schema_version!==1 || descriptor.module_id!=='muse'
      || !isRecord(artifact) || artifact.artifact_id!==MUSE_ARTIFACT_ID
      || artifact.version!==MUSE_ARTIFACT_VERSION || !isRecord(protocol)
      || !sameJson(protocol.minimum,{major:1,minor:0})
      || !sameJson(protocol.maximum,{major:1,minor:0})
      || !Array.isArray(descriptor.capabilities)
      || !Array.isArray(descriptor.command_schemas) || !Array.isArray(descriptor.event_schemas))
    throw new Error('MODULE_DESCRIPTOR_IDENTITY_INVALID');
  const capabilities=[...descriptor.capabilities].sort(compareText);
  if(new Set(capabilities).size!==capabilities.length || !capabilities.includes('task.dispatch'))
    throw new Error('MODULE_DESCRIPTOR_CAPABILITIES_INVALID');
  const commandSchemas=descriptor.command_schemas.map(claimSchema).sort((a,b)=>
    compareText(a.schema_id,b.schema_id)||compareText(a.version,b.version)||compareText(a.sha256??'',b.sha256??''));
  const eventSchemas=descriptor.event_schemas.map(claimSchema).sort((a,b)=>
    compareText(a.schema_id,b.schema_id)||compareText(a.version,b.version)||compareText(a.sha256??'',b.sha256??''));
  const hasExactSchema=(schemas,id,version)=>schemas.some(schema=>schema.schema_id===id
    && schema.version===version && schema.sha256==null);
  if(!hasExactSchema(commandSchemas,'swarm.runtime_command','1')
      || !hasExactSchema(commandSchemas,'swarm.task_prompt','1')
      || !hasExactSchema(commandSchemas,'swarm.task_dispatch_context','1')
      || !hasExactSchema(eventSchemas,'swarm.runtime_outcome','1')
      || !hasExactSchema(eventSchemas,'swarm.task_dispatch_admission','1'))
    throw new Error('MODULE_DESCRIPTOR_TASK_PROMPT_CONTRACT_INVALID');
  const claimArtifact={artifact_id:artifact.artifact_id,version:artifact.version,
    ...(artifact.build_id==null?{}:{build_id:artifact.build_id})};
  return {
    schema_version:1,
    module_id:descriptor.module_id,
    artifact:claimArtifact,
    protocol:{major:1,minor:0},
    capabilities,
    config_schema:descriptor.config_schema??null,
    ...(descriptor.pre_input_open==null?{}:{pre_input_open:descriptor.pre_input_open}),
    command_schemas:commandSchemas,
    event_schemas:eventSchemas,
  };
}
const MUSE_MODULE_CONTRACT=moduleContractClaim(museDescriptor);
function requireModuleContractNegotiated(hello) {
  const negotiation=hello?.module_contract_negotiation;
  const fields=['status','source','descriptor_revision','module_id','artifact','protocol','capabilities',
    'config_schema','pre_input_open','command_schemas','event_schemas','effects_authorized_by_descriptor'];
  if(!exactFields(negotiation,fields) || negotiation.status!=='negotiated'
      || negotiation.source!=='store_registered_descriptor'
      || !Number.isSafeInteger(negotiation.descriptor_revision) || negotiation.descriptor_revision<=0
      || negotiation.effects_authorized_by_descriptor!==false)
    throw new Error('MODULE_CONTRACT_NEGOTIATION_REQUIRED');
  const expected={module_id:MUSE_MODULE_CONTRACT.module_id,artifact:MUSE_MODULE_CONTRACT.artifact,
    protocol:MUSE_MODULE_CONTRACT.protocol,capabilities:MUSE_MODULE_CONTRACT.capabilities,
    config_schema:MUSE_MODULE_CONTRACT.config_schema,pre_input_open:MUSE_MODULE_CONTRACT.pre_input_open??null,
    command_schemas:MUSE_MODULE_CONTRACT.command_schemas,event_schemas:MUSE_MODULE_CONTRACT.event_schemas};
  const actual={module_id:negotiation.module_id,artifact:negotiation.artifact,protocol:negotiation.protocol,
    capabilities:negotiation.capabilities,config_schema:negotiation.config_schema,
    pre_input_open:negotiation.pre_input_open,command_schemas:negotiation.command_schemas,
    event_schemas:negotiation.event_schemas};
  if(!sameJson(actual,expected))throw new Error('MODULE_CONTRACT_NEGOTIATION_MISMATCH');
}
function validIdentity(value, maxBytes = 512) {
  return typeof value === 'string' && value.trim().length > 0
    && Buffer.byteLength(value,'utf8') <= maxBytes && !/\p{Cc}/u.test(value);
}
function moduleReceipt(command) {
  const inputSha256=command?.input_sha256;
  if(!validIdentity(command?.operation_id) || !validIdentity(command?.binding_id)
      || !Number.isSafeInteger(command?.generation) || command.generation<=0
      || command?.route?.module_artifact_id!==MUSE_ARTIFACT_ID || !SHA256.test(inputSha256??'')) return null;
  return {
    schema_version:1,
    module_id:'muse',
    artifact:{artifact_id:MUSE_ARTIFACT_ID,version:MUSE_ARTIFACT_VERSION,build_id:null},
    protocol:{major:1,minor:0},
    binding_id:command.binding_id,
    binding_generation:command.generation,
    operation_id:command.operation_id,
    input_sha256:inputSha256,
  };
}
function taskPromptFor(command) {
  const input=command?.input;
  const envelope=input?.task_prompt;
  const context=input?.task_dispatch_context;
  const sourceText=input?.text;
  const sourceBytes=typeof sourceText==='string'?Buffer.byteLength(sourceText,'utf8'):-1;
  if(!exactFields(envelope,TASK_PROMPT_FIELDS) || !exactFields(context,TASK_DISPATCH_CONTEXT_FIELDS)
      || envelope.schema_id!=='swarm.task_prompt' || envelope.schema_version!==1
      || !validIdentity(envelope.task_id) || !validIdentity(envelope.attempt_id)
      || !Number.isSafeInteger(envelope.task_revision) || envelope.task_revision<=0
      || !SHA256.test(envelope.task_snapshot_sha256) || !SHA256.test(envelope.prompt_sha256)
      || typeof envelope.prompt!=='string' || !envelope.prompt.trim()
      || !Number.isSafeInteger(envelope.prompt_bytes) || envelope.prompt_bytes<=0
      || envelope.prompt_bytes>MAX_TASK_PROMPT_BYTES
      || Buffer.byteLength(envelope.prompt,'utf8')!==envelope.prompt_bytes
      || sha256(Buffer.from(envelope.prompt,'utf8'))!==envelope.prompt_sha256
      || context.schema_version!==1 || !validIdentity(context.operation_id)
      || !validIdentity(context.binding_id) || !validIdentity(context.worker_boot_id)
      || !validIdentity(context.attempt_id) || !validIdentity(context.task_id)
      || !Number.isSafeInteger(context.binding_generation) || context.binding_generation<=0
      || !Number.isSafeInteger(context.task_revision) || context.task_revision<=0
      || !SHA256.test(context.task_snapshot_sha256) || !SHA256.test(context.source_text_sha256)
      || !Number.isSafeInteger(context.source_text_bytes) || context.source_text_bytes<=0
      || typeof sourceText!=='string' || !sourceText.trim()
      || context.operation_id!==command.operation_id || context.binding_id!==command.binding_id
      || context.binding_generation!==command.generation || context.worker_boot_id!==bootId
      || context.source_text_bytes!==sourceBytes
      || context.source_text_sha256!==sha256(Buffer.from(sourceText,'utf8'))
      || context.attempt_id!==envelope.attempt_id || context.task_id!==envelope.task_id
      || context.task_revision!==envelope.task_revision
      || context.task_snapshot_sha256!==envelope.task_snapshot_sha256) {
    throw new Error('TASK_PROMPT_INVALID');
  }
  return {envelope,context};
}
function taskDispatchAdmission(entry) {
  const {envelope,context}=entry.taskPrompt;
  const receipt=moduleReceipt(entry.command);
  if(!receipt)throw new Error('HOST_COMMAND_IDENTITY_INVALID');
  return {
    schema_version:context.schema_version,
    module_receipt:receipt,
    operation_id:context.operation_id,
    binding_id:context.binding_id,
    binding_generation:context.binding_generation,
    worker_boot_id:context.worker_boot_id,
    attempt_id:context.attempt_id,
    task_id:context.task_id,
    task_revision:context.task_revision,
    task_snapshot_sha256:context.task_snapshot_sha256,
    source_text_sha256:context.source_text_sha256,
    source_text_bytes:context.source_text_bytes,
    native_payload_sha256:envelope.prompt_sha256,
    native_payload_bytes:envelope.prompt_bytes,
    native_input_id:entry.id,
  };
}
function commandId(command) {
  // Minted once per Operation and persisted before native I/O; the
  // derivation itself lives in observe.mjs so fixtures can pin it.
  return commandIdFor(command.operation_id, command.created_at_ms);
}
function compactSession(s) {
  if (!s || typeof s.sessionId !== 'string' || typeof s.status !== 'string') throw new Error('INVALID_SESSION');
  return { sessionId:s.sessionId, status:s.status, activeTurnId:s.activeTurnId,
    modelId:s.modelId, providerId:s.providerId, approvalMode:s.approvalMode, attention:s.attention, turnCount:s.turnCount };
}
const SESSION_LIST_LIMIT=200;
class NativeCompatibilityError extends Error {
  constructor(diagnosticCode,nativeError) {
    super(diagnosticCode);
    this.name='NativeCompatibilityError';
    this.diagnosticCode=diagnosticCode;
    if(nativeError instanceof MspError)this.nativeError=nativeError;
  }
}
function nativeErrorOf(error) {
  if(error instanceof MspError)return error;
  return error instanceof NativeCompatibilityError?error.nativeError:null;
}
function diagnosticCode(error,fallback='NATIVE_ERROR') {
  if(error instanceof NativeCompatibilityError)return error.diagnosticCode;
  if(error instanceof MspError)return 'NATIVE_ERROR';
  return typeof error?.message==='string'&&/^[A-Z0-9_]{1,120}$/.test(error.message)?error.message:fallback;
}
function methodNotFound(error) {
  const native=nativeErrorOf(error);
  return native?.code===-32601||native?.kind==='methodNotFound';
}
function validateNativeSession(session,sessionId,{allowNotLoaded=false}={}) {
  if(!isRecord(session)||session.sessionId!==sessionId)throw new NativeCompatibilityError('NATIVE_SESSION_READBACK_IDENTITY_MISMATCH');
  if(!['notLoaded','idle','running'].includes(session.status))throw new NativeCompatibilityError('NATIVE_SESSION_READBACK_STATUS_UNKNOWN');
  if(!Object.hasOwn(session,'activeTurnId')
      || session.activeTurnId!==null&&(typeof session.activeTurnId!=='string'||!session.activeTurnId))
    throw new NativeCompatibilityError('NATIVE_SESSION_READBACK_STATE_INVALID');
  if(session.status==='idle'&&session.activeTurnId!==null
      || session.status==='running'&&typeof session.activeTurnId!=='string')
    throw new NativeCompatibilityError('NATIVE_SESSION_READBACK_STATE_INVALID');
  if(session.status==='notLoaded'&&!allowNotLoaded)throw new NativeCompatibilityError('NATIVE_SESSION_READBACK_NOT_LOADED');
  if(session.modelId!==null&&session.modelId!==undefined&&typeof session.modelId!=='string')
    throw new NativeCompatibilityError('NATIVE_SESSION_READBACK_MODEL_INVALID');
  return session;
}
async function readListedSession(sessionId,{allowNotLoaded=false}={}) {
  if(!msp)throw new Error('NATIVE_NOT_OPEN');
  const workspaceRoot=routeDefaults.workspaceRoot;
  if(typeof workspaceRoot!=='string'||!path.isAbsolute(workspaceRoot))
    throw new NativeCompatibilityError('NATIVE_SESSION_LIST_WORKSPACE_UNAVAILABLE');
  let result;
  try {
    result=await msp.connection.request('session/list',{workspaceRoot,limit:SESSION_LIST_LIMIT});
  } catch(error) {
    if(methodNotFound(error))throw new NativeCompatibilityError('NATIVE_SESSION_LIST_UNSUPPORTED',nativeErrorOf(error));
    throw error;
  }
  if(!Array.isArray(result?.sessions)||!(result.nextCursor===null||typeof result.nextCursor==='string'))
    throw new NativeCompatibilityError('NATIVE_SESSION_LIST_RESPONSE_INVALID');
  const matches=result.sessions.filter(session=>session?.sessionId===sessionId);
  if(matches.length>1)throw new NativeCompatibilityError('NATIVE_SESSION_LIST_IDENTITY_AMBIGUOUS');
  if(matches.length===0)throw new NativeCompatibilityError(result.nextCursor
    ?'NATIVE_SESSION_LIST_READBACK_INCOMPLETE':'NATIVE_SESSION_LIST_SESSION_MISSING');
  return validateNativeSession(matches[0],sessionId,{allowNotLoaded});
}
let sessionReadUnavailable=false;
async function readNativeSession(sessionId,{allowNotLoaded=false}={}) {
  if(!msp)throw new Error('NATIVE_NOT_OPEN');
  if(latest.host_durability?.profile==='durable'&&!sessionReadUnavailable) {
    try {
      const result=await msp.connection.request('session/read',{sessionId,excludeItems:true});
      const session=validateNativeSession(result?.session,sessionId,{allowNotLoaded});
      if(typeof result.viewCursor!=='string'||!result.viewCursor)
        throw new NativeCompatibilityError('NATIVE_SESSION_READBACK_CURSOR_MISSING');
      return {session,method:'session/read',viewCursor:result.viewCursor};
    } catch(error) {
      if(!methodNotFound(error))throw error;
      sessionReadUnavailable=true;
    }
  }
  return {session:await readListedSession(sessionId,{allowNotLoaded}),method:'session/list'};
}
function sessionResumeUnavailableCode() {
  return latest.host_durability?.profile==='ephemeral'
    ?'NATIVE_SESSION_RESUME_UNSUPPORTED_EPHEMERAL':'NATIVE_SESSION_RESUME_DURABILITY_UNKNOWN';
}
const argv = process.argv.slice(2);
if (argv.length !== 2 || argv[0] !== '--config') {
  console.error('Usage: node bridge.mjs --config <local-module.json>'); process.exit(2);
}
const config = JSON.parse(await readFile(argv[1], 'utf8'));
const credential = JSON.parse(await readFile(required(config, 'credentialFile'), 'utf8'));
required(config, 'endpoint'); required(config, 'command'); required(config, 'moduleArtifactId');
if (config.moduleArtifactId !== MUSE_ARTIFACT_ID) throw new Error('MODULE_ARTIFACT_MISMATCH');
if (!path.isAbsolute(config.command)) throw new Error('NATIVE_EXECUTABLE_MUST_BE_ABSOLUTE');
if (process.platform === 'win32' && /\.(cmd|bat)$/i.test(config.command)) throw new Error('USE_NATIVE_EXE_NOT_SHELL_WRAPPER');
if (!Array.isArray(config.args) || config.args.some(a => typeof a !== 'string')) throw new Error('EXPLICIT_ARGV_REQUIRED');

const recovery = await recoveryState();
const saved = recovery?.saved;
if(saved && (saved.client_id!==credential.client_id || saved.module_artifact_id!==config.moduleArtifactId))throw new Error('CHECKPOINT_BINDING_MISMATCH');
const bootId = recovery?.owner.token ?? randomUUID();
let control, connected = false, stopping = false, nativeReady = false, handshake, msp, rootId, nativeScope;
let revision = 0, lastSentRevision = -1, eventsSeen = 0, savedRevision = -1;
let nativeUsageConnectionId, nativeUsageGeneration = 0, nativeConnectionExited = false;
let latest = { execution: 'not_started', family_completeness: 'partial', observed_children: [], pending_requests: [], gaps: 0 };
const children = new Map(), pendingRequests = new Map(), outcomes = new Map(), active = new Map();
const routeDefaults = {};
const turns = new Map();
// Only unconfirmed native commands live here. Exact payload/ID is reused only
// through explicit agent.reconcile, never through reconnect or a timeout loop.
const nativePending = new Map();
const sessionVersions = new Map();
let standingEffort, refreshInFlight, bindingContext;
const childReads = new Map();
if(saved) {
  rootId=saved.root_id;nativeScope=saved.native_scope;
  Object.assign(routeDefaults,saved.route_defaults);
  standingEffort=saved.standing_effort;
  bindingContext=saved.binding;
  for(const [id,value] of saved.outcomes??[])outcomes.set(id,value);
  for(const [id,value] of saved.native_pending??[])nativePending.set(id,value);
  for(const [id,value] of saved.children??[]) {
    // A checkpoint preserves evidence, not currentness. Keep every recorded
    // child identity and snapshot, but require a successful exact read in
    // this bridge boot before calling that snapshot fresh again.
    const hasExactSnapshot=value?.snapshot?.sessionId===id;
    children.set(id,{...value,snapshot_freshness:hasExactSnapshot?'stale':'unknown',
      snapshot_freshness_reason:hasExactSnapshot?'checkpoint_restored':'checkpoint_snapshot_unverified'});
  }
  for(const [id,value] of saved.turns??[])turns.set(id,value);
  latest={...saved.latest,execution:'recovery_required',gaps:(saved.latest?.gaps??0)+1,
    family_completeness:'partial',recovered_checkpoint:true};
  delete latest.usage;
  const restoredUsage=restoredNativeUsageSnapshot(latest.native_usage);
  if(restoredUsage)latest.native_usage=restoredUsage;
  else delete latest.native_usage;
}
function checkpoint(force=false) {
  if(!recovery || !force && savedRevision===revision)return Promise.resolve();
  const at=revision;
  return recovery.write({client_id:credential.client_id,module_artifact_id:config.moduleArtifactId,
    boot_id:bootId,binding:bindingContext,root_id:rootId,native_scope:nativeScope,
    route_defaults:routeDefaults,standing_effort:standingEffort,latest,
    outcomes:[...outcomes],native_pending:[...nativePending],children:[...children],turns:[...turns]}).then(()=>{savedRevision=at;});
}
function saveOutcome(operationId, result, command) {
  const pending = nativePending.get(operationId);
  if (pending?.resolved && !['applied','rejected'].includes(result.outcome)) return;
  const old = outcomes.get(operationId);
  if (old && ['applied','rejected'].includes(old.outcome)) return;
  const receipt=moduleReceipt(command??pending?.command);
  const details={...(result.details??{}),...(receipt?{module_receipt:receipt}:{})};
  outcomes.set(operationId, {operation_id:operationId,...result,details}); changed();
}
function settle(entry, details, turnId) {
  entry.resolved = true;
  const admission=entry.taskPrompt?taskDispatchAdmission(entry):undefined;
  saveOutcome(entry.command.operation_id, {outcome:'applied',native_root_id:rootId,native_scope_key:nativeScope,
    ...(turnId?{turn_id:turnId}:{}),...(admission?{native_input_id:entry.id}:{}),
    details:{...details,...(admission?{dispatch_admission:admission}:{})}},entry.command);
}
function acceptModel(entry, session, evidence) {
  if (!entry.ack || entry.setting?.key !== 'model' || !modelMatches(session, entry.setting.desired)) return false;
  retainModelOverride(routeDefaults, entry.setting.desired);
  latest.model = {modelId:session.modelId,providerId:session.providerId};
  settle(entry, {native_ack:entry.ack,completion_condition:'native_configuration_applied',settings:entry.setting.desired,evidence});
  return true;
}

function changed() { revision++; }
function invalidateChildSnapshot(id, reason) {
  const child=children.get(id);
  if (!child || child.snapshot_freshness!=='fresh') return false;
  children.set(id,{...child,snapshot_freshness:'stale',snapshot_freshness_reason:reason});
  changed();
  return true;
}
function sessionChanged(sessionId, reason = 'native_event_after_read') {
  if (typeof sessionId !== 'string' || !sessionId) return;
  sessionVersions.set(sessionId, (sessionVersions.get(sessionId)??0)+1);
  invalidateChildSnapshot(sessionId, reason);
}
function workFenceMatchesSession(fence, session) {
  return session?.sessionId===fence.rootId
    && session.modelId===fence.modelId
    && session.providerId===fence.providerId
    && session.status===fence.status
    && session.activeTurnId===fence.activeTurnId;
}
function workFenceCurrent(fence, entry, eventVersion=fence.eventVersion) {
  return rootId===fence.rootId
    && entry.command.native_root_id===fence.rootId
    && entry.params?.sessionId===fence.rootId
    && (sessionVersions.get(fence.rootId)??0)===eventVersion
    && (routeDefaults.modelId===undefined || routeDefaults.modelId===fence.modelId)
    && (routeDefaults.providerId===undefined || routeDefaults.providerId===fence.providerId);
}
function hasPendingModelConfiguration() {
  return [...nativePending.values()].some(entry=>entry.kind==='configure'
    && entry.setting?.key==='model' && !entry.resolved);
}
async function readWorkFence(modelChangedCode) {
  // setModel's ACK is admission only. Keep every productive path behind the
  // unresolved request until its exact requested selector is read back; a
  // restored checkpoint carries the same fence across bridge restart.
  if(hasPendingModelConfiguration())
    throw new NativeCompatibilityError('MODEL_CONFIGURATION_RECONCILIATION_REQUIRED');
  const sessionId=rootId;
  if(!sessionId)throw new NativeCompatibilityError('NATIVE_ROOT_IDENTITY_UNAVAILABLE');
  const eventVersion=sessionVersions.get(sessionId)??0;
  const read=await readNativeSession(sessionId);
  if(rootId!==sessionId || read.session.sessionId!==sessionId
      || eventVersion!==(sessionVersions.get(sessionId)??0))
    throw new NativeCompatibilityError('NATIVE_WORK_SNAPSHOT_CHANGED_DURING_READ');
  if(!modelMatches(read.session,routeDefaults))throw new NativeCompatibilityError(modelChangedCode);
  return {rootId:sessionId,eventVersion,modelId:read.session.modelId,providerId:read.session.providerId,
    status:read.session.status,activeTurnId:read.session.activeTurnId};
}
function rejectWorkFence(entry, code) {
  entry.workFenceRejected=true;
  entry.workFenceRejectionCode=code;
  changed();
}
function beginNativeUsageConnection() {
  nativeUsageConnectionId=randomUUID();
  nativeUsageGeneration=1;
  latest.native_usage=emptyNativeUsageSnapshot({connectionId:nativeUsageConnectionId,method:'usage/read',
    revision:nativeUsageGeneration,collectedAtMs:Date.now(),completeness:'not_observed'});
  changed();
}
function recordNativeUsageChanged(value) {
  if(!nativeUsageConnectionId)return;
  nativeUsageGeneration++;
  latest.native_usage=nativeUsageSnapshot({connectionId:nativeUsageConnectionId,method:'usage/changed',
    revision:nativeUsageGeneration,collectedAtMs:Date.now(),value});
}
async function readNativeUsage() {
  const connectionId=nativeUsageConnectionId;
  if(!connectionId || !msp)return;
  const generation=nativeUsageGeneration;
  try {
    const result=await msp.connection.request('usage/read',{});
    if(connectionId!==nativeUsageConnectionId || generation!==nativeUsageGeneration)return;
    nativeUsageGeneration++;
    latest.native_usage=nativeUsageReadSnapshot({connectionId,revision:nativeUsageGeneration,
      collectedAtMs:Date.now(),result});
  } catch(error) {
    if(connectionId!==nativeUsageConnectionId || generation!==nativeUsageGeneration)return;
    nativeUsageGeneration++;
    const unsupported=error instanceof MspError && error.kind==='methodNotFound';
    latest.native_usage=emptyNativeUsageSnapshot({connectionId,method:'usage/read',revision:nativeUsageGeneration,
      collectedAtMs:Date.now(),completeness:unsupported?'unsupported':'not_observed'});
  }
  changed();
}
function closeNativeUsageConnection() {
  const connectionId=nativeUsageConnectionId;
  if(!connectionId)return;
  nativeUsageGeneration++;
  nativeUsageConnectionId=undefined;
  latest.native_usage=closedNativeUsageSnapshot(latest.native_usage,{connectionId,revision:nativeUsageGeneration,
    collectedAtMs:Date.now()});
}
function pendingRequestKey(sessionId, kind, requestId) {
  if (typeof sessionId !== 'string' || !sessionId) throw new Error('PENDING_REQUEST_SESSION_REQUIRED');
  if (!['approval','input'].includes(kind)) throw new Error('PENDING_REQUEST_KIND_INVALID');
  if (typeof requestId !== 'string' || !requestId) throw new Error('PENDING_REQUEST_ID_REQUIRED');
  // Native request IDs are scoped by their session on every reply/read API.
  // Do not let a root and child overwrite one another in the bridge inventory.
  return JSON.stringify([sessionId,kind,requestId]);
}
function recordPendingRequestGap(gap) {
  const retained=Array.isArray(latest.pending_request_gaps)?latest.pending_request_gaps:[];
  latest.pending_request_gaps=[...retained.slice(-31),gap];
  latest.gaps++;
  changed();
}
function replacePendingInventory(sessionId, inventory, expectedVersion) {
  if (!Array.isArray(inventory?.approvals) || !Array.isArray(inventory?.userInputs)) throw new Error('INVALID_PENDING_INVENTORY');
  // Validate the entire point-in-time inventory before removing live facts.
  // This synchronous commit cannot interleave with a native event handler.
  const replacement = new Map();
  for (const [items, prefix, idField, method] of [
    [inventory.approvals, 'approval', 'approvalId', 'approval/request'],
    [inventory.userInputs, 'input', 'userInputId', 'userInput/request'],
  ]) {
    for (const params of items) {
      if (params?.sessionId !== sessionId) throw new Error('PENDING_INVENTORY_IDENTITY_MISMATCH');
      const key = pendingRequestKey(sessionId, prefix, required(params,idField));
      if (replacement.has(key)) throw new Error('PENDING_INVENTORY_DUPLICATE_ID');
      replacement.set(key, {view:{method,params}});
    }
  }
  if (expectedVersion !== (sessionVersions.get(sessionId)??0)) return false;
  for (const [key, entry] of pendingRequests) if (entry.view.params?.sessionId === sessionId) pendingRequests.delete(key);
  for (const [key, entry] of replacement) pendingRequests.set(key, entry);
  return true;
}
function observation() {
  return { ...latest, native_root_id:rootId, native_scope_key:nativeScope,
    observed_children:[...children.values()], pending_requests:[...pendingRequests.values()].map(x=>x.view),
    boot_id:bootId, native_events_seen:eventsSeen, turns:[...turns.values()] };
}
function pendingRequestIdentitySummary(view) {
  const params = isRecord(view?.params) ? view.params : {};
  const method=view?.method;
  const idField=method==='approval/request'?'approvalId'
    :method==='userInput/request'?'userInputId':null;
  if(!idField || typeof params.sessionId!=='string' || !params.sessionId
      || typeof params[idField]!=='string' || !params[idField]) return null;
  return {method,params:{sessionId:params.sessionId,[idField]:params[idField]},
    request_details_state:'unknown'};
}
function pendingRequestIdentityDigest(requests) {
  const digest = createHash('sha256');
  for (const view of requests) {
    const params = isRecord(view?.params) ? view.params : {};
    const idField=view?.method==='approval/request'?'approvalId'
      :view?.method==='userInput/request'?'userInputId':null;
    digest.update(JSON.stringify([view?.method ?? null,
      typeof params.sessionId==='string'?params.sessionId:null,
      idField && typeof params[idField]==='string'?params[idField]:null]));
    digest.update('\n');
  }
  return digest.digest('hex');
}
function incompleteGapProjection(gap, omitSessionId = false) {
  const hasSessionId=typeof gap?.session_id==='string';
  const sessionIdentityOmitted=hasSessionId&&omitSessionId;
  return {
    status:'unfilled',
    reason:gap?.reason,
    session_id:hasSessionId&&!omitSessionId?gap.session_id:null,
    after:null,
    next:null,
    cursors_complete:false,
    ...(Number.isSafeInteger(gap?.at_ms)?{at_ms:gap.at_ms}:{}),
    projection_state:'incomplete',
    projection_reason:'opaque_gap_bracket_did_not_fit_complete_observation_frame',
    projection_omission:sessionIdentityOmitted
      ?'opaque_cursor_bracket_and_session_id':'opaque_cursor_bracket',
    ...(sessionIdentityOmitted?{session_identity_omitted:true}:{}),
  };
}
function boundedObservation(state, link, callParams) {
  const stateBytes = () => Buffer.byteLength(JSON.stringify(state));
  const callFor = candidate => ({ ...callParams, state:candidate });
  const frameBytes = candidate => link.frameByteLength('module.observe',callFor(candidate));
  const fits = candidate => frameBytes(candidate) <= link.limit;
  if (stateBytes() <= MAX_OBSERVATION_STATE_BYTES && fits(state)) return state;

  const requests=Array.isArray(state.pending_requests)?state.pending_requests:[];
  const sourceChildren=Array.isArray(state.observed_children)?state.observed_children:[];
  const sourceTurns=Array.isArray(state.turns)?state.turns:[];
  const summaries=requests.map(pendingRequestIdentitySummary);
  const gapCount=Number.isSafeInteger(state.gaps)&&state.gaps>=0?state.gaps+1:1;

  // Retain the established bounded child tail and completeness diagnostic.
  state.observed_children=sourceChildren.slice(-100);
  state.gaps=gapCount;
  state.family_completeness='partial';
  if (fits(state)) return state;

  // The native map remains authoritative. Only this outbound copy drops the
  // large request details; each summary keeps the exact addressable identity.
  state.pending_requests=summaries.filter(Boolean);
  if (state.pending_requests.length===requests.length && fits(state)) return state;

  // If the aggregate view is still too large, start from a small honest
  // projection, then keep every individual identity that fits. Each request
  // is considered independently so one overlarge identity cannot suppress a
  // later fitting sibling. Omitted identities stay pending in pendingRequests.
  const compact={
    execution:typeof state.execution==='string'&&Buffer.byteLength(state.execution)<=128
      ?state.execution:'unknown',
    family_completeness:'partial',
    gaps:gapCount,
    boot_id:state.boot_id,
    pending_requests:[],
    observation_projection_state:'partial',
  };
  const identityMetadata=requests.length>0;
  if(identityMetadata){
    compact.pending_requests_state='partial';
    compact.pending_requests_count=requests.length;
    compact.pending_requests_omitted_count=requests.length;
    compact.pending_requests_identity_sha256=pendingRequestIdentityDigest(requests);
  }
  const sourceGapFill=state.gap_fill;
  if(isRecord(sourceGapFill)&&sourceGapFill.status==='unfilled'){
    // Keep the complete opaque bracket when it fits this compact frame. If it
    // does not, retain an explicit unresolved marker; never trim one cursor or
    // mutate the authoritative latest.gap_fill object.
    compact.gap_fill=sourceGapFill;
    if(!fits(compact)){
      compact.gap_fill=incompleteGapProjection(sourceGapFill);
      if(!fits(compact))compact.gap_fill=incompleteGapProjection(sourceGapFill,true);
      if(!fits(compact))throw new Error('OBSERVATION_FRAME_UNREPRESENTABLE');
    }
  }
  let measured=frameBytes(compact);
  if(measured>link.limit)throw new Error('OBSERVATION_FRAME_UNREPRESENTABLE');
  let visibleCount=0;
  for(const summary of summaries){
    if(!summary)continue;
    const identityBytes=Buffer.byteLength(JSON.stringify(summary));
    const separatorBytes=visibleCount===0?0:1;
    const oldOmitted=requests.length-visibleCount;
    const newOmitted=oldOmitted-1;
    const omittedCountDelta=Buffer.byteLength(String(newOmitted))-Buffer.byteLength(String(oldOmitted));
    const delta=identityBytes+separatorBytes+omittedCountDelta;
    if(measured+delta>link.limit)continue;
    compact.pending_requests.push(summary);
    visibleCount++;
    measured+=delta;
    compact.pending_requests_omitted_count=newOmitted;
  }
  const omittedCount=requests.length-visibleCount;
  if(identityMetadata&&omittedCount===0){
    delete compact.pending_requests_state;
    delete compact.pending_requests_count;
    delete compact.pending_requests_omitted_count;
    delete compact.pending_requests_identity_sha256;
  }

  const tryAdd=(key,value)=>{
    compact[key]=value;
    if(fits(compact))return true;
    delete compact[key];
    return false;
  };
  const reserved=new Set(['execution','family_completeness','gaps','boot_id','pending_requests',
    'pending_requests_state','pending_requests_count','pending_requests_omitted_count',
    'pending_requests_identity_sha256','observation_projection_state','native_root_id',
    'native_scope_key','native_events_seen','observed_children','turns','gap_fill']);
  for(const key of ['native_root_id','native_scope_key','native_events_seen']){
    if(Object.hasOwn(state,key))tryAdd(key,state[key]);
  }
  const omittedFields=[];
  for(const [key,value] of Object.entries(state)){
    if(reserved.has(key))continue;
    if(!tryAdd(key,value))omittedFields.push(key);
  }
  const addRows=(key,rows)=>{
    if(!Array.isArray(rows))return 0;
    const selected=[];
    let omitted=0;
    for(const row of rows.slice(-100)){
      selected.push(row);
      compact[key]=selected;
      if(!fits(compact)){selected.pop();omitted++;}
    }
    if(selected.length===0)delete compact[key];
    return Math.max(0,rows.length-100)+omitted;
  };
  const omittedChildren=addRows('observed_children',sourceChildren);
  const omittedTurns=addRows('turns',sourceTurns);
  if(omittedChildren>0)omittedFields.push('observed_children');
  if(omittedTurns>0)omittedFields.push('turns');
  if(omittedFields.length)tryAdd('observation_projection_omitted_fields',[...new Set(omittedFields)]);
  if(!fits(compact))throw new Error('OBSERVATION_FRAME_UNREPRESENTABLE');
  return compact;
}
function onNotification(n) {
  const p = n.params ?? {};
  // Do not duplicate token streams, tool output or the SDK's full transcript.
  if (n.method === 'item/delta') return;
  eventsSeen++;
  sessionChanged(p.sessionId??p.session?.sessionId,
    n.method==='view/gap'||n.method==='session/viewHealthChanged'
      ?'native_view_gap_after_read':'native_event_after_read');
  // A correlated event can resolve a lost turn admission reply. It never
  // submits another prompt and never claims that the Task was accepted.
  const inputCommand = p.commandId ?? p.item?.commandId;
  const observedTurn = p.turnId ?? p.item?.turnId;
  if (inputCommand && observedTurn && p.sessionId === rootId
      && (n.method === 'turn/started' || n.method === 'turn/completed'
          || (n.method === 'item/completed' && p.item?.kind === 'userMessage'))) {
    for (const entry of nativePending.values()) {
      if (entry.id === inputCommand && entry.kind === 'input') {
        settle(entry, {completion_condition:'native_input_admitted',evidence:{method:n.method,commandId:inputCommand,turnId:observedTurn}}, observedTurn);
      }
    }
  }
  if (n.method === 'view/gap' || n.method === 'session/viewHealthChanged') {
    latest.gaps++; latest.view_health = { method:n.method, sessionId:p.sessionId, status:p.status ?? p.health ?? 'unknown' };
    // A gap is also recorded as an unfilled hole in its own right: this
    // bridge runs the compact-observation path, not the SDK facade's
    // splice-fill, so the inability and its opaque bracket are facts the
    // operator can see instead of only a counter going up.
    if (n.method === 'view/gap') latest.gap_fill = gapFillObservation(p, Date.now());
  }
  if (['turn/started','turn/completed','turn/unqueued'].includes(n.method)) {
    if (typeof p.sessionId !== 'string' || typeof p.turnId !== 'string' || !p.turnId) { latest.gaps++; changed(); return; }
    const state = { sessionId:p.sessionId, turnId:p.turnId, commandId:p.commandId,
      event:n.method, terminal:n.method==='turn/unqueued'?'cancelled':p.terminal, viewCursor:p.viewCursor,
      sourceRange:p.sourceRange, errorKind:p.error?.kind };
    const key = `${p.sessionId}:${p.turnId}`;
    if (n.method === 'turn/started' && turns.get(key)?.terminal) return;
    turns.set(key, state);
    while (turns.size > 256) { turns.delete(turns.keys().next().value); latest.gaps++; }
    if (p.sessionId === rootId) {
      if (n.method === 'turn/started') latest.active_turn_id = p.turnId;
      else { if (latest.active_turn_id === p.turnId) latest.active_turn_id = null; latest.last_completed_turn = state; }
    }
    else if (children.has(p.sessionId)) children.set(p.sessionId, { ...children.get(p.sessionId), last_turn:state });
  }
  if (n.method === 'session/statusChanged' && p.sessionId === rootId) {
    latest.execution = p.status; latest.attention = p.attention;
  }
  const item = p.item;
  if (['subagent','reminderChild'].includes(item?.kind) && typeof item.childSessionId==='string'
      && typeof item.itemId==='string' && Number.isSafeInteger(item.revision)
      && (p.sessionId === rootId || children.has(p.sessionId))) {
    if (children.has(item.childSessionId) || children.size < 2000) {
      const old = children.get(item.childSessionId);
      // Replayed equal revisions are historical evidence, not a newer parent
      // declaration. A changed item ID is a distinct assignment even when the
      // native session ID is reused.
      if (!old || item.itemId !== old.itemId || !Number.isSafeInteger(old.revision) || item.revision > old.revision) {
        const wasFresh=old?.snapshot_freshness==='fresh';
        children.set(item.childSessionId, { ...old, sessionId:item.childSessionId, parentSessionId:p.sessionId,
          itemId:item.itemId, subagentId:item.subagentId, parentTurnId:item.turnId,
          status:item.status, controlStatus:item.controlStatus, revision:item.revision,
          snapshot_freshness:wasFresh?'stale':old?.snapshot_freshness??'unknown',
          snapshot_freshness_reason:wasFresh?'parent_child_event_after_read':old?.snapshot_freshness_reason??'native_read_not_yet_verified',
          result_available:Boolean(item.result),
          result_summary:typeof item.result?.summary==='string'?item.result.summary.slice(0,512):undefined,
          result_error_kind:item.result?.errorKind,
          result_source:item.result?{parentSessionId:p.sessionId,itemId:item.itemId,revision:item.revision,viewCursor:p.viewCursor}:undefined });
      }
      if (!old && msp) {
        // Subscribe from an observed read cursor; never acquire a child's writer lease.
        void followChild(item.childSessionId);
      }
    } else latest.gaps++;
  }
  if (n.method === 'session/goalChanged' && p.sessionId === rootId) latest.goal = p.goal ?? null;
  if (n.method === 'session/reasoningEffortChanged' && p.sessionId === rootId) { latest.reasoning_effort = p.reasoningEffort; standingEffort = p.reasoningEffort; }
  if (n.method === 'usage/changed') recordNativeUsageChanged(p);
  if (n.method === 'session/modelChanged' && p.sessionId === rootId) {
    latest.model = { modelId:p.modelId,providerId:p.providerId };
    for (const entry of nativePending.values()) acceptModel(entry, p, {method:n.method,viewCursor:p.viewCursor,sourceRange:p.sourceRange});
  }
  if (n.method === 'approval/requested' || n.method === 'approval/updated' || n.method === 'userInput/requested') {
    const sessionId=required(p,'sessionId');
    const kind=n.method.startsWith('approval/')?'approval':'input';
    const requestId=kind==='approval'?required(p,'approvalId'):required(p,'userInputId');
    const key=pendingRequestKey(sessionId,kind,requestId);
    let params=p;
    if (n.method === 'approval/updated') {
      const previous=pendingRequests.get(key)?.view.params;
      // An update contains stage fields, not the original actionable request.
      // Preserve the gap rather than fabricating tool/request identity.
      if (!previous || previous.sessionId !== sessionId) {
        recordPendingRequestGap({code:'PENDING_APPROVAL_UPDATE_WITHOUT_REQUEST',session_id:sessionId,
          approval_id:requestId,current_requirement_id:p.currentRequirementId??null,view_cursor:p.viewCursor??null});
        return;
      }
      params={...previous,...p};
      if (p.subagentOrigin === undefined) delete params.subagentOrigin;
    }
    pendingRequests.set(key,{view:{method:n.method,params}});
  }
  if (n.method === 'approval/resolved' || n.method === 'userInput/settled') {
    const sessionId=required(p,'sessionId');
    const kind=n.method==='approval/resolved'?'approval':'input';
    const requestId=kind==='approval'?required(p,'approvalId'):required(p,'userInputId');
    pendingRequests.delete(pendingRequestKey(sessionId,kind,requestId));
  }
  changed();
}
async function refreshRoot() {
  if (refreshInFlight) return refreshInFlight;
  refreshInFlight = (async () => {
    if (!msp || !rootId) throw new Error('NATIVE_NOT_OPEN');
    const before = sessionVersions.get(rootId)??0;
    const read = await readNativeSession(rootId);
    const snapshot = compactSession(read.session);
    // Live events received during the read take precedence. No ordering is
    // invented by parsing opaque native cursors.
    const fresh = before === (sessionVersions.get(rootId)??0);
    if (fresh) {
      latest.session = snapshot; latest.execution = snapshot.status;
      latest.active_turn_id = snapshot.activeTurnId;
      const evidence={method:read.method,...(read.viewCursor?{viewCursor:read.viewCursor}:{updatedAt:read.session.updatedAt})};
      for (const entry of nativePending.values()) acceptModel(entry, snapshot, evidence);
    }
    const pendingAt = sessionVersions.get(rootId)??0;
    const pending = await msp.connection.request('approval/listPending', {sessionId:rootId});
    const pendingApplied = replacePendingInventory(rootId, pending, pendingAt);
    latest.last_refresh = {at_ms:Date.now(),metadata_applied:fresh,pending_inventory_applied:pendingApplied,
      readback_method:read.method,...(read.viewCursor?{viewCursor:read.viewCursor}:{}),
      coverage:'root_metadata_and_pending_requests'};
    changed();
    return latest.last_refresh;
  })();
  try { return await refreshInFlight; } finally { refreshInFlight = undefined; }
}

async function completeNative(entry, ack) {
  entry.ack = ack;
  await checkpoint(true);
  if(entry.kind==='recover') {
    const session=compactSession(ack.session);
    if(session.sessionId!==rootId)throw new Error('RESUMED_IDENTITY_MISMATCH');
    if(!modelMatches(session,routeDefaults))throw new Error('MODEL_CHANGED_DURING_RECOVERY');
    const current=entry.submission_boot_id===bootId;
    if(current) {
      latest.session=session;latest.execution=session.status;latest.active_turn_id=session.activeTurnId;
      latest.recovery={status:'resumed',operation_id:entry.command.operation_id,viewCursor:ack.viewCursor};
      nativeReady=true;
    }
    settle(entry,{completion_condition:'native_session_resumed',resume_boot_id:entry.submission_boot_id,
      native_command_id:entry.id,viewCursor:ack.viewCursor,session,model_input_replayed:false});
    await checkpoint();
    // Restoring observation is read-only, separate from the admitted resume.
    if(current) {
      void refreshRoot().catch(()=>{latest.gaps++;changed();});
      for(const id of children.keys())void followChild(id);
    }
    return;
  }
  if (ack.status !== 'accepted') throw new Error('UNEXPECTED_ADMISSION_STATUS');
  if (entry.kind === 'input') {
    settle(entry, {native_ack:ack,completion_condition:'native_input_admitted',
      requested_reasoning_effort:entry.params.reasoningEffort??null,
      reasoning_application:entry.params.reasoningEffort!==undefined?'explicit_per_turn':'native_default'}, required(ack,'turnId'));
  } else if (entry.kind === 'configure') {
    const setting = entry.setting;
    if (setting.key === 'reasoningEffort') {
      // This setter's documented ack is durable and applies to subsequently
      // launched turns. That native guarantee differs from setModel's ack.
      standingEffort = setting.desired; routeDefaults.reasoningEffort = setting.desired;
      latest.reasoning_effort = setting.desired;
      latest.reasoning_application = 'native_durable_default_ack_and_explicit_per_turn';
      settle(entry, {native_ack:ack,completion_condition:'native_configuration_applied',boundary:'subsequent_turns',evidence_kind:'native_durable_ack',reasoningEffort:setting.desired});
    } else if (setting.key === 'approvalMode') {
      if (ack.effectiveMode?.mode !== setting.desired) throw new Error('APPROVAL_MODE_READBACK_MISMATCH');
      routeDefaults.approvalMode = setting.desired; latest.approval_mode = ack.effectiveMode;
      settle(entry, {native_ack:ack,completion_condition:'native_configuration_applied',boundary:'next_action'});
    } else {
      saveOutcome(entry.command.operation_id, {outcome:'accepted',native_root_id:rootId,native_scope_key:nativeScope,
        details:{native_ack:ack,completion_condition:'native_configuration_applied',waiting_for:'model_readback',requested:setting.desired}},entry.command);
      await refreshRoot();
      // If no matching observation exists yet, preserve accepted/pending. New
      // model work remains serialized behind it while replies/refresh continue.
    }
  } else {
    if (entry.kind === 'goal') latest.goal_admission = {action:entry.command.input.action,native_ack:ack};
    settle(entry, {native_ack:ack,completion_condition:entry.kind==='goal'?'native_goal_admitted':'native_reply_admitted'}, ack.turnId);
  }
}

async function submitNative(command, method, params, kind, setting, taskPrompt, beforeNativeRequest, workFence) {
  const entry = {command,id:commandId(command),method,params,kind,setting,taskPrompt,ack:null,resolved:false,
    submission_boot_id:bootId,...(workFence?{workFence}:{})};
  nativePending.set(command.operation_id, entry);
  await checkpoint(true); // Native mutation must not precede its recoverable ID/payload.
  try { beforeNativeRequest?.(entry); }
  catch(error) {
    if(workFence) {
      rejectWorkFence(entry,diagnosticCode(error,'NATIVE_WORK_SNAPSHOT_CHANGED_BEFORE_REQUEST'));
      await checkpoint(true);
    }
    throw error;
  }
  const ack = await msp.connection.command(method, params, {maxAttempts:1,commandId:entry.id});
  await completeNative(entry, ack);
}

async function reconcileNative(target) {
  const entry = nativePending.get(target);
  if (!entry) throw new Error('RECONCILIATION_CONTEXT_UNAVAILABLE');
  if (active.has(target)) return {target_operation_id:target,disposition:'native_request_in_progress',resubmitted:false};
  if (entry.resolved) return {target_operation_id:target,disposition:'awaiting_host_receipt',resubmitted:false};
  if (entry.ack) {
    await completeNative(entry,entry.ack);
    return {target_operation_id:target,disposition:entry.resolved?'resolved':'pending_application',resubmitted:false};
  }
  // This durable fact means the stored intent never reached MSP. Preserve it
  // for inspection, but do not turn explicit reconciliation into a new send.
  if(entry.workFenceRejected)return {target_operation_id:target,disposition:'work_fence_rejected_before_native_request',
    resubmitted:false,diagnostic_code:entry.workFenceRejectionCode??'NATIVE_WORK_SNAPSHOT_CHANGED_BEFORE_REQUEST'};
  if(entry.method==='session/resume'&&latest.host_durability?.profile!=='durable')
    throw new NativeCompatibilityError(sessionResumeUnavailableCode());
  // This is an explicit reconciliation command, not a read-only healthcheck.
  // SDK/server idempotency joins the SAME command ID with byte-equivalent params.
  try {
    if(entry.workFence) {
      const fence=entry.workFence;
      const eventVersion=sessionVersions.get(fence.rootId)??0;
      const read=await readNativeSession(fence.rootId);
      const snapshotMatches=workFenceMatchesSession(fence,read.session)
        && modelMatches(read.session,routeDefaults);
      // The read above yielded; recheck its event version immediately before
      // the exact same-ID request, including on a recovered pending entry.
      if(!snapshotMatches || !workFenceCurrent(fence,entry,eventVersion)) {
        rejectWorkFence(entry,'NATIVE_WORK_SNAPSHOT_CHANGED_BEFORE_RECONCILIATION');
        await checkpoint(true);
        return {target_operation_id:target,disposition:'work_fence_rejected_before_native_request',resubmitted:false,
          diagnostic_code:entry.workFenceRejectionCode};
      }
    }
    const ack = await msp.connection.command(entry.method, entry.params, {maxAttempts:1,commandId:entry.id});
    await completeNative(entry, ack);
  } catch (error) {
    const native=nativeErrorOf(error);
    saveOutcome(target, {outcome:failedReconcileOutcome(native?{code:native.code,kind:native.kind}:null, Boolean(entry.ack)),native_root_id:rootId,native_scope_key:nativeScope,
      details:{evidence_kind:'explicit_same_command_reconciliation',native_code:native?.code??null,native_kind:native?.kind??null,
        diagnostic_code:diagnosticCode(error)}},entry.command);
    throw error;
  }
  return {target_operation_id:target,disposition:entry.resolved?'resolved':'pending_application',resubmitted:true,native_command_id:entry.id};
}

async function refreshChild(id) {
  if (childReads.has(id)) return childReads.get(id);
  if (!children.has(id)) throw new Error('CHILD_OUTSIDE_OBSERVED_FAMILY');
  const pending = (async () => {
    let stage='session_read';
    try {
      let snapshot,readbackMethod,viewCursor,subscriptionViewCursor,baseline;
      const canReadStored=latest.host_durability?.profile==='durable'&&!sessionReadUnavailable;
      if(canReadStored) {
        const before=sessionVersions.get(id)??0;
        let read;
        try {
          read=await msp.connection.request('session/read',{sessionId:id,excludeItems:true});
        } catch(error) {
          if(!methodNotFound(error))throw error;
          sessionReadUnavailable=true;
        }
        if(read) {
          validateNativeSession(read.session,id,{allowNotLoaded:true});
          viewCursor=required(read,'viewCursor');
          snapshot=compactSession(read.session);readbackMethod='session/read';baseline=before;
          // Subscribe from the read cursor, then verify no event arrived during
          // either subscription replay or pending-request inventory.
          stage='view_subscribe';
          await msp.connection.request('view/subscribe',{sessionId:id,after:viewCursor});
        }
      }
      if(!snapshot) {
        // Ephemeral hosts intentionally withhold session/read. Subscribe from
        // the current head first, then list the exact loaded row. If an event
        // arrives during the list/pending window, retain the prior stale state.
        stage='view_subscribe';
        const subscribed=await msp.connection.request('view/subscribe',{sessionId:id});
        subscriptionViewCursor=required(subscribed,'viewCursor');
        baseline=sessionVersions.get(id)??0;
        stage='session_list';
        const session=await readListedSession(id);
        snapshot=compactSession(session);readbackMethod='session/list';
      }
      const pendingAt = sessionVersions.get(id)??0;
      stage='pending_inventory';
      const questions = await msp.connection.request('approval/listPending',{sessionId:id});
      const pendingApplied = replacePendingInventory(id, questions, pendingAt);
      const fresh = baseline===(sessionVersions.get(id)??0);
      const refreshed={at_ms:Date.now(),session_id:id,metadata_applied:fresh,pending_inventory_applied:pendingApplied,
        readback_method:readbackMethod,...(viewCursor?{viewCursor}:{subscription_view_cursor:subscriptionViewCursor}),
        coverage:'observed_child_metadata_and_pending_requests'};
      const current=children.get(id);
      if (fresh) {
        children.set(id,{...current,snapshot,snapshot_freshness:'fresh',snapshot_freshness_reason:readbackMethod==='session/list'
          ?'stable_session_list_after_view_subscription':'stable_native_read',
          last_refresh:refreshed,last_refresh_attempt:{...refreshed,status:'applied'}});
      } else {
        children.set(id,{...current,snapshot_freshness:current?.snapshot?'stale':'unknown',snapshot_freshness_reason:'native_event_during_refresh',
          last_refresh_attempt:{...refreshed,status:'raced',snapshot_retained:true}});
      }
      changed();
      return refreshed;
    } catch(error) {
      // Preserve the exact child identity, last snapshot, and last successful
      // refresh as stale evidence. A failed/missing read only changes the
      // currentness marker and records a bounded diagnostic for this attempt.
      const current=children.get(id);
      const native=nativeErrorOf(error);
      children.set(id,{...current,snapshot_freshness:current?.snapshot?'stale':'unknown',snapshot_freshness_reason:'native_read_failed',
        last_refresh_attempt:{at_ms:Date.now(),status:'failed',stage,
          error_type:error?.name??'Error',
          native_code:native?.code??undefined,native_kind:native?.kind??undefined,
          diagnostic_code:diagnosticCode(error,'NATIVE_READ_FAILED'),
          snapshot_retained:Boolean(current?.snapshot)}});
      changed();
      throw error;
    }
  })();
  childReads.set(id,pending);
  try {return await pending;} finally {childReads.delete(id);}
}
async function followChild(id) {
  try {await refreshChild(id);} catch {latest.gaps++;changed();}
}
async function launchConnection(options) {
  if (handshake) throw new Error('NATIVE_ALREADY_OWNED');
  required(options,'workspaceRoot');
  if (!path.isAbsolute(options.workspaceRoot)) throw new Error('WORKSPACE_MUST_BE_ABSOLUTE');
  const allowed=['workspaceRoot','modelId','providerId','reasoningEffort','approvalMode','mcpServers'];
  for (const name of Object.keys(options)) if (!allowed.includes(name)) throw new Error(`UNSUPPORTED_NATIVE_OPTION_${name}`);
  // Omission selects the installed native default (installed msp.d.ts
  // SessionStartParams: modelId optional 'server default when omitted',
  // providerId optional-nullable 'server default when omitted or explicit null';
  // TurnStartParams reasoningEffort optional, absent means unspecified for the
  // turn, not a reset). Explicit values are validated verbatim; no default is
  // synthesized here and no provider credential/config is read.
  if (options.modelId !== undefined
    && (typeof options.modelId !== 'string' || !options.modelId.trim() || options.modelId !== options.modelId.trim()))
    throw new Error('INVALID_MODEL_SELECTION');
  if (options.providerId !== undefined && options.providerId !== null
    && (typeof options.providerId !== 'string' || !options.providerId.trim() || options.providerId !== options.providerId.trim()))
    throw new Error('INVALID_PROVIDER');
  if (options.reasoningEffort !== undefined
    && !['none','minimal','low','medium','high','xhigh','max','ultra'].includes(options.reasoningEffort))
    throw new Error('INVALID_REASONING_EFFORT');
  if (options.mcpServers !== undefined) nativeMcpServers(options.mcpServers);
  // Explicit launch only, never from describe/status/reconnect. Current vendor auth is inherited.
  sessionReadUnavailable=false;
  handshake=(recovery?spawnOwned:spawnMspConnection)({command:config.command,args:config.args,cwd:options.workspaceRoot,
    onStderr:()=>{latest.stderr_chunks=(latest.stderr_chunks??0)+1;}});
  handshake.onNotification(onNotification);
  handshake.onProtocolError(()=>{latest.gaps++;latest.protocol_error=true;changed();});
  handshake.onServerRequest(request=>{
    // MSP RequestReceipt is {}, not an approval decision. Retain the question
    // for GM and acknowledge presentation immediately; commands carry answers.
    if(!['approval/request','userInput/request'].includes(request.method)) {
      throw new MspError({code:-32601,message:'Unsupported server request',data:{kind:'methodNotFound'}});
    }
    const p=request.params ?? {};
    const sessionId=required(p,'sessionId');
    const kind=request.method==='approval/request'?'approval':'input';
    const key=pendingRequestKey(sessionId,kind,kind==='approval'?required(p,'approvalId'):required(p,'userInputId'));
    // Server requests and notifications invalidate the same read window.
    sessionChanged(sessionId, 'native_request_after_read');
    pendingRequests.set(key,{view:{method:request.method,params:p}}); changed();
    return {};
  });
  handshake.exited.then(exit=>{nativeReady=false;nativeConnectionExited=true;latest.execution='native_exited';latest.exit=exit;
    latest.host_death=hostDeathObservation(latest.host_durability?.profile,exit,Date.now());closeNativeUsageConnection();changed();},
    ()=>{nativeReady=false;nativeConnectionExited=true;latest.execution='native_failed';
    latest.host_death=hostDeathObservation(latest.host_durability?.profile,null,Date.now());closeNativeUsageConnection();changed();});
  msp=await handshake.initialize({clientInfo:{name:'eliot_swarm_controller',version:'0.1.0'},capabilities:{requestedCapabilities:['sessionMcp']}});
  beginNativeUsageConnection();
  const init=msp.initializeResult;
  const home=await realpath(required(init,'museHome'));
  const scope=`muse:${process.platform}:${process.platform==='win32'?home.toLowerCase():home}`;
  if(nativeScope && nativeScope!==scope)throw new Error('RECOVERY_NAMESPACE_MISMATCH');
  nativeScope=scope;
  latest.server=init.serverInfo; latest.schema=init.schema;
  latest.granted_capabilities=Array.isArray(init.grantedCapabilities)?init.grantedCapabilities.filter(name=>typeof name==="string"):[];
  // The handshake's own durability declaration, classified per SS2.13.1.
  // Recorded at every launch; a restored checkpoint keeps the previous
  // connection's profile until a new handshake replaces it.
  latest.host_durability=durabilityProfile(init);
  latest.fingerprint_warning=Boolean(msp.fingerprintWarning);
  if(nativeConnectionExited)closeNativeUsageConnection();
  else void readNativeUsage();
  return init;
}

async function startNative(command,markNativeAdmissionPossible) {
  const options=command.route.native_options ?? {};
  if (typeof options.workspaceRoot !== 'string' || !options.workspaceRoot.trim())
    throw new Error('WORKSPACE_MUST_BE_ABSOLUTE');
  // A new root replaces the previous route defaults. Omitted model/provider/
  // effort stay omitted so the native default applies. Observed values belong
  // to session readback; they must not become controller route overrides.
  for (const name of ['modelId','providerId','reasoningEffort']) delete routeDefaults[name];
  Object.assign(routeDefaults,options);
  if(routeDefaults.providerId===null)delete routeDefaults.providerId;
  const mcpServers=options.mcpServers===undefined?undefined:nativeMcpServers(options.mcpServers);
  delete latest.mcp_servers_requested;
  delete latest.mcp_servers_config_forwarded;
  delete latest.mcp_servers_admission_ack;
  if(mcpServers!==undefined){
    latest.mcp_servers_requested=Object.keys(mcpServers);
    latest.mcp_servers_config_forwarded=null;
    latest.mcp_servers_admission_ack=null;
  }
  await checkpoint(true);
  const init=await launchConnection(options);
  // Omit unset model/provider so the installed server default applies.
  // Explicit values are forwarded byte-exact. A null providerId is also an
  // omission spelling per the installed schema; it is omitted on the wire.
  const params={workspaceRoot:options.workspaceRoot};
  if(options.modelId!==undefined)params.modelId=options.modelId;
  if(options.providerId!==undefined && options.providerId!==null)params.providerId=options.providerId;
  if(options.approvalMode!==undefined)params.approvalMode=options.approvalMode;
  if(mcpServers!==undefined){
    // The pinned facade omits config until its grant check lands; this raw
    // Connection frame is the only documented forwarding path (installed 1.4.0
    // refuses unknown transports at decode, verified memory-only, no turn).
    if(!(Array.isArray(latest.granted_capabilities)&&latest.granted_capabilities.includes('sessionMcp')))throw new Error('SESSION_MCP_NOT_GRANTED');
  }
  const r=await startNativeMcpSession(msp.connection,params,
    {maxAttempts:1,commandId:commandId(command)},mcpServers,latest,markNativeAdmissionPossible);
  rootId=required(r.session,'sessionId'); latest.session=compactSession(r.session);latest.execution=r.session.status;
  // Persist the root identity before any further native work or readback
  // verdict: a crash after session/start must leave a recorded session that
  // explicit recovery can target, not an unrecorded one known only in memory.
  changed(); await checkpoint(true);
  // Explicit selections must read back exactly. Omitted selections remain
  // omitted; the session snapshot records native observations separately.
  if(options.modelId!==undefined) {
    if(r.session.modelId!==options.modelId)throw new Error('MODEL_READBACK_MISMATCH');
  }
  if(options.providerId!==undefined && options.providerId!==null) {
    if(r.session.providerId!==options.providerId)throw new Error('PROVIDER_READBACK_MISMATCH');
  }
  if(options.reasoningEffort===undefined) delete routeDefaults.reasoningEffort;
  latest.model={modelId:r.session.modelId,providerId:r.session.providerId};
  latest.requested_model=options.modelId??null;
  latest.requested_model_status=options.modelId!==undefined?'explicit':'omitted_native_default';
  latest.requested_provider=options.providerId??null;
  latest.requested_provider_status=options.providerId!==undefined&&options.providerId!==null?'explicit':'omitted_native_default';
  latest.requested_reasoning_effort=options.reasoningEffort??null;
  latest.reasoning_application=options.reasoningEffort!==undefined
    ?'explicit_per_turn_option' // Sampled at submission, not a claimed inference measurement.
    :'native_default_at_submission';
  nativeReady=true;changed();
  await checkpoint();
  return {native_scope_key:nativeScope,native_root_id:rootId,details:{session:latest.session,server:init.serverInfo,mcp:{
    granted_capabilities:Array.isArray(latest.granted_capabilities)?[...latest.granted_capabilities]:[],
    requested_server_names:latest.mcp_servers_requested??null,
    config_forwarded_server_names:latest.mcp_servers_config_forwarded??null,
    session_start_admission_ack:latest.mcp_servers_admission_ack??null},
    requested:{model:latest.requested_model,model_status:latest.requested_model_status,
      provider:latest.requested_provider,provider_status:latest.requested_provider_status},
    observed:{modelId:r.session.modelId,providerId:r.session.providerId},
    reasoning:{requested:latest.requested_reasoning_effort,application:latest.reasoning_application},fingerprint_warning:Boolean(msp.fingerprintWarning)}};
}
async function recoverNative(command,markNativeAdmissionPossible) {
  if(!recovery || !rootId || !nativeScope)throw new Error('RECOVERY_CHECKPOINT_UNAVAILABLE');
  if(command.input.expected_boot_id!==bootId || command.native_root_id!==rootId)throw new Error('RECOVERY_TARGET_CHANGED');
  if(nativeReady)throw new Error('NATIVE_ALREADY_READY');
  if(latest.host_durability?.profile==='ephemeral')throw new NativeCompatibilityError(sessionResumeUnavailableCode());
  if(!handshake)await launchConnection(routeDefaults);
  if(!msp)throw new Error('NATIVE_INITIALIZE_UNKNOWN');
  if(latest.host_durability?.profile!=='durable')throw new NativeCompatibilityError(sessionResumeUnavailableCode());
  // Check the retained root before taking a writer lease. Never replace a
  // missing session with a new one, fork, or an initial Task prompt.
  const read=await readNativeSession(rootId,{allowNotLoaded:true});
  const session=compactSession(read.session);
  if(!modelMatches(session,routeDefaults)) {
    const expected=[...nativePending.values()].find(entry=>entry.kind==='configure' && entry.setting?.key==='model' && modelMatches(session,entry.setting.desired));
    if(!expected)throw new Error('RECOVERY_MODEL_MISMATCH');
    retainModelOverride(routeDefaults,expected.setting.desired);
    await checkpoint(true);
  }
  await submitNative(command,'session/resume',{sessionId:rootId,excludeItems:true},'recover',undefined,undefined,
    markNativeAdmissionPossible);
}
async function execute(command) {
  const base={operation_id:command.operation_id};
  let nativeAdmissionPossible=false;
  try {
    if(!moduleReceipt(command))throw new Error('HOST_COMMAND_IDENTITY_INVALID');
    let result;
    if (command.method==='agent.open') { result=await startNative(command,()=>{nativeAdmissionPossible=true;}); }
    else if(command.method==='agent.recover') {await recoverNative(command,()=>{nativeAdmissionPossible=true;});return;}
    else {
      if (!msp || command.native_root_id!==rootId) throw new Error('NATIVE_IDENTITY_MISMATCH');
      const p=command.input;
      if (command.method==='task.dispatch' || command.method==='agent.send') {
        const taskPrompt=command.method==='task.dispatch'?taskPromptFor(command):undefined;
        const workFence=await readWorkFence('MODEL_CHANGED_BEFORE_SEND');
        const steer=p.delivery==='steer';
        const promptText=taskPrompt?taskPrompt.envelope.prompt:required(p,'text');
        // Omit per-turn reasoningEffort when no standing default is set so
        // the native default applies; an explicit standing value rides verbatim.
        const input={sessionId:rootId,input:[{type:'text',text:promptText}]};
        if(routeDefaults.reasoningEffort!==undefined)input.reasoningEffort=routeDefaults.reasoningEffort;
        if(steer)input.expectedTurnId=required(p,'expected_turn_id');else input.ifBusy='queue';
        await submitNative(command,steer?'turn/steer':'turn/start',input,'input',undefined,taskPrompt,entry=>{
          if(!workFenceCurrent(workFence,entry))
            throw new NativeCompatibilityError('NATIVE_WORK_SNAPSHOT_CHANGED_BEFORE_REQUEST');
          nativeAdmissionPossible=true;
        },workFence);
        return;
      } else if(command.method==='agent.reply') {
        const r=p.reply;
        const method=required(r,'method');
        if(!['approval/decide','userInput/answer','userInput/cancel','userInput/clarify'].includes(method))throw new Error('UNSUPPORTED_REPLY_METHOD');
        const params=r.params;
        if(!params || typeof params!=='object' || !(params.sessionId===rootId || children.has(params.sessionId)))throw new Error('REPLY_OUTSIDE_OBSERVED_FAMILY');
        nativeAdmissionPossible=true;
        await submitNative(command,method,params,'reply');
        return;
      } else if (command.method === 'agent.configure') {
        const setting = configuration(p.settings, rootId);
        nativeAdmissionPossible = true;
        await submitNative(command,setting.method,setting.params,'configure',setting);
        return;
      } else if (command.method === 'agent.goal') {
        const goal = goalCommand(p,rootId);
        let workFence,beforeNativeRequest;
        if (goal.startsWork) {
          // With no route override, goals use the native standing default.
          // Native observations do not create a requested effort constraint.
          if (routeDefaults.reasoningEffort !== undefined && standingEffort !== routeDefaults.reasoningEffort)
            throw new Error('CONFIGURE_STANDING_EFFORT_BEFORE_GOAL');
          workFence=await readWorkFence('MODEL_CHANGED_BEFORE_GOAL');
          beforeNativeRequest=entry=>{
            if(!workFenceCurrent(workFence,entry))
              throw new NativeCompatibilityError('NATIVE_WORK_SNAPSHOT_CHANGED_BEFORE_REQUEST');
            nativeAdmissionPossible=true;
          };
        } else nativeAdmissionPossible = true;
        await submitNative(command,goal.method,goal.params,'goal',undefined,undefined,beforeNativeRequest,workFence);
        return;
      } else if (command.method === 'agent.result') {
        const page = await readResult(msp.connection,p,id=>id===rootId || children.has(id));
        result = {result_page:page,details:{completion_condition:'result_page_pending_persistence'}};
      } else if (command.method === 'agent.refresh') {
        const target = p.session_id===undefined?rootId:required(p,'session_id');
        result = {details:{completion_condition:'native_read_completed',snapshot:await (target===rootId?refreshRoot():refreshChild(target))}};
      } else if (command.method === 'agent.reconcile') {
        nativeAdmissionPossible = true;
        result = {details:await reconcileNative(required(p,'operation_id'))};
      } else throw new Error('UNSUPPORTED_OPERATION');
      result.native_root_id=rootId;result.native_scope_key=nativeScope;
    }
    saveOutcome(command.operation_id,{outcome:'applied',...result},command);
  } catch(error) {
    // Compatibility and method-not-found failures prove no native method ran.
    // After admission, only the pinned durable commandRejected code/kind settles
    // native work; other errors remain unknown for explicit same-ID reconciliation.
    const native=nativeErrorOf(error);
    const rejected=!nativeAdmissionPossible || error instanceof NativeCompatibilityError || methodNotFound(error)
      || native && (native.code===-32030 || native.kind==='commandRejected');
    const accepted = nativePending.get(command.operation_id)?.ack;
    const outcome={...base,outcome:accepted?'accepted':rejected?'rejected':'unknown',details:{error_type:error.name,
      native_kind:native?.kind??null,native_code:native?.code??null,diagnostic_code:diagnosticCode(error,String(error.message).slice(0,120))}};
    if(rootId){outcome.native_root_id=rootId;outcome.native_scope_key=nativeScope;}
    saveOutcome(command.operation_id,outcome,command);
  } finally {
    active.delete(command.operation_id);
    if(nativePending.get(command.operation_id)?.hostAcknowledged)nativePending.delete(command.operation_id);
    changed();
  }
}
async function report(link = control) {
  await checkpoint(); // Retain outcomes before allowing a host ACK to retire them.
  for(const [id,outcome] of outcomes){
    if(outcome.result_page) await link.call('module.result',{operation_id:id,page:outcome.result_page});
    else await link.call('module.outcome',outcome);
    // A later native event may have resolved this operation while IPC awaited.
    // Do not erase the newer outcome with the old acknowledgement.
    if(outcomes.get(id)===outcome){
      outcomes.delete(id);
      if(['applied','rejected'].includes(outcome.outcome)) {
        const entry=nativePending.get(id);
        if(entry)entry.hostAcknowledged=true;
        if(!active.has(id))nativePending.delete(id);
      }
    }
  }
  if(lastSentRevision!==revision){
    const at=revision;
    const callParams={event_id:`${bootId}:${at}`,sequence:at};
    const state=boundedObservation(observation(),link,callParams);
    const params={...callParams,state};
    if(link.frameByteLength('module.observe',params)>link.limit)
      throw new Error('OBSERVATION_FRAME_TOO_LARGE');
    await link.call('module.observe',params);lastSentRevision=at;
  }
}
let admissionTail=Promise.resolve();
let reportBusy=false;
const interval=setInterval(()=>{
  if(!connected || reportBusy)return;
  reportBusy=true;
  const link=control;
  void report(link).catch(()=>link.close()).finally(()=>{reportBusy=false;});
},1000);
interval.unref();
async function stop(){
  if(stopping)return;stopping=true;connected=false;control?.close();clearInterval(interval);
  // SDK close ends this owned transport/process. The public MSP surface has no
  // session/close request; do not fabricate one or infer session settlement here.
  await checkpoint(true).catch(()=>{});
  if(handshake)await handshake.close().catch(()=>{});
}
process.once('SIGINT',()=>{void stop();});process.once('SIGTERM',()=>{void stop();});
while(!stopping){
  try{
    control=new Control(config.endpoint,credential);await control.connect();
    const hello=await control.call('module.hello',{boot_id:bootId,module_artifact_id:config.moduleArtifactId,
      module_contract:MUSE_MODULE_CONTRACT,native_ready:nativeReady,
      ...(recovery?{managed_owner:recovery.owner}:{}),
      ...(rootId?{native_root_id:rootId,native_scope_key:nativeScope}:{})});
    requireModuleContractNegotiated(hello);
    if(bindingContext && (hello.binding_id!==bindingContext.binding_id || hello.generation!==bindingContext.generation))throw new Error('CHECKPOINT_BINDING_CHANGED');
    bindingContext={binding_id:hello.binding_id,generation:hello.generation};
    if(hello.recovery_required)latest.recovery={status:'awaiting_explicit_agent_recover',boot_id:bootId};
    await checkpoint(true);
    lastSentRevision=-1;await report();connected=true;
    while(!stopping && control.socket){
      if(active.size+outcomes.size>=8){await delay(50);continue;}
      const result=await control.call('module.next',{});
      if(result.command){
        const command=result.command;
        if(!active.has(command.operation_id) && !outcomes.has(command.operation_id)) {
          active.set(command.operation_id,true);
          // Serialize native admission, not whole model turns. Replies bypass this
          // queue so an outstanding command cannot deadlock a native question.
          if(['agent.reply','agent.refresh','agent.reconcile','agent.result'].includes(command.method)) void execute(command);
          else admissionTail=admissionTail.then(()=>execute(command));
        }
      }
    }
  } catch(error) {
    // A rejected new boot requires operator reconciliation, not repeated process spawning.
    console.error(JSON.stringify({component:'muse-bridge',code:error.code??error.message,native_preserved:Boolean(handshake)}));
  } finally{connected=false;control?.close();}
  if(!stopping)await delay(1000);
}
