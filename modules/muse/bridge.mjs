#!/usr/bin/env node
// SDK-owned native execution. Host IPC reconnect never closes Muse or repeats input.
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { spawnMspConnection, MspError } from '@muse-code/sdk';
import { Control } from './control.mjs';
import { configuration, modelMatches, goalCommand } from './settings.mjs';

function required(object, key) {
  if (typeof object?.[key] !== 'string' || !object[key].trim()) throw new Error(`MISSING_${key}`);
  return object[key];
}
function commandId(command) {
  // A stable UUIDv7-shaped native ID, distinct from our transport request IDs.
  // The native schema requires UUIDv7; the controller operation ID is UUIDv4.
  const bytes = createHash('sha256').update(command.operation_id).digest().subarray(0, 16);
  bytes.writeUIntBE(command.created_at_ms, 0, 6);
  bytes[6] = (bytes[6] & 15) | 0x70; bytes[8] = (bytes[8] & 63) | 0x80;
  const h = bytes.toString('hex'); return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;
}
function compactSession(s) {
  if (!s || typeof s.sessionId !== 'string' || typeof s.status !== 'string') throw new Error('INVALID_SESSION');
  return { sessionId:s.sessionId, status:s.status, activeTurnId:s.activeTurnId,
    modelId:s.modelId, providerId:s.providerId, approvalMode:s.approvalMode, attention:s.attention, turnCount:s.turnCount };
}
const argv = process.argv.slice(2);
if (argv.length !== 2 || argv[0] !== '--config') {
  console.error('Usage: node bridge.mjs --config <local-module.json>'); process.exit(2);
}
const config = JSON.parse(await readFile(argv[1], 'utf8'));
const credential = JSON.parse(await readFile(required(config, 'credentialFile'), 'utf8'));
required(config, 'endpoint'); required(config, 'command'); required(config, 'moduleArtifactId');
if (!path.isAbsolute(config.command)) throw new Error('NATIVE_EXECUTABLE_MUST_BE_ABSOLUTE');
if (process.platform === 'win32' && /\.(cmd|bat)$/i.test(config.command)) throw new Error('USE_NATIVE_EXE_NOT_SHELL_WRAPPER');
if (!Array.isArray(config.args) || config.args.some(a => typeof a !== 'string')) throw new Error('EXPLICIT_ARGV_REQUIRED');

const bootId = randomUUID();
let control, connected = false, stopping = false, nativeReady = false, handshake, msp, rootId, nativeScope;
let revision = 0, lastSentRevision = -1, eventsSeen = 0;
let latest = { execution: 'not_started', family_completeness: 'partial', observed_children: [], pending_requests: [], gaps: 0 };
const children = new Map(), pendingRequests = new Map(), outcomes = new Map(), active = new Map();
const routeDefaults = {};
const turns = new Map();
// Only unconfirmed native commands live here. Exact payload/ID is reused only
// through explicit agent.reconcile, never through reconnect or a timeout loop.
const nativePending = new Map();
const sessionVersions = new Map();
let standingEffort, refreshInFlight;
const childReads = new Map();
function saveOutcome(operationId, result) {
  const pending = nativePending.get(operationId);
  if (pending?.resolved && !['applied','rejected'].includes(result.outcome)) return;
  const old = outcomes.get(operationId);
  if (old && ['applied','rejected'].includes(old.outcome)) return;
  outcomes.set(operationId, {operation_id:operationId,...result}); changed();
}
function settle(entry, details, turnId) {
  entry.resolved = true;
  saveOutcome(entry.command.operation_id, {outcome:'applied',native_root_id:rootId,native_scope_key:nativeScope,
    ...(turnId?{turn_id:turnId}:{}), details});
}
function acceptModel(entry, session, evidence) {
  if (!entry.ack || entry.setting?.key !== 'model' || !modelMatches(session, entry.setting.desired)) return false;
  routeDefaults.modelId = session.modelId;
  routeDefaults.providerId = session.providerId ?? undefined;
  latest.model = {modelId:session.modelId,providerId:session.providerId};
  settle(entry, {native_ack:entry.ack,completion_condition:'native_configuration_applied',settings:entry.setting.desired,evidence});
  return true;
}

function changed() { revision++; }
function observation() {
  return { ...latest, native_root_id:rootId, native_scope_key:nativeScope,
    observed_children:[...children.values()], pending_requests:[...pendingRequests.values()].map(x=>x.view),
    boot_id:bootId, native_events_seen:eventsSeen, turns:[...turns.values()] };
}
function onNotification(n) {
  const p = n.params ?? {};
  // Do not duplicate token streams, tool output or the SDK's full transcript.
  if (n.method === 'item/delta') return;
  eventsSeen++;
  if (p.sessionId) sessionVersions.set(p.sessionId, (sessionVersions.get(p.sessionId)??0)+1);
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
      if (!old || item.itemId !== old.itemId || Number(item.revision ?? 0) >= Number(old.revision ?? 0)) {
        children.set(item.childSessionId, { ...old, sessionId:item.childSessionId, parentSessionId:p.sessionId,
          itemId:item.itemId, subagentId:item.subagentId, parentTurnId:item.turnId,
          status:item.status, controlStatus:item.controlStatus, revision:item.revision,
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
  if (n.method === 'usage/changed') latest.usage = { basis:'native_snapshot', value:p };
  if (n.method === 'session/modelChanged' && p.sessionId === rootId) {
    latest.model = { modelId:p.modelId,providerId:p.providerId };
    for (const entry of nativePending.values()) acceptModel(entry, p, {method:n.method,viewCursor:p.viewCursor,sourceRange:p.sourceRange});
  }
  if (n.method === 'approval/requested' || n.method === 'approval/updated' || n.method === 'userInput/requested') {
    const key = p.approvalId ? `approval:${p.approvalId}` : `input:${p.userInputId}`;
    pendingRequests.set(key, {view:{method:n.method,params:p}});
  }
  if (n.method === 'approval/resolved' || n.method === 'userInput/settled') {
    for (const [key,entry] of pendingRequests) {
      if (entry.view.params?.approvalId === p.approvalId && p.approvalId || entry.view.params?.userInputId === p.userInputId && p.userInputId) pendingRequests.delete(key);
    }
  }
  changed();
}
async function refreshRoot() {
  if (refreshInFlight) return refreshInFlight;
  refreshInFlight = (async () => {
    if (!msp || !rootId) throw new Error('NATIVE_NOT_OPEN');
    const before = sessionVersions.get(rootId)??0;
    const r = await msp.connection.request('session/read', {sessionId:rootId,excludeItems:true});
    const snapshot = compactSession(r.session);
    if (snapshot.sessionId !== rootId) throw new Error('SNAPSHOT_IDENTITY_MISMATCH');
    // Live events received during the read take precedence. No ordering is
    // invented by parsing opaque native cursors.
    const fresh = before === (sessionVersions.get(rootId)??0);
    if (fresh) {
      latest.session = snapshot; latest.execution = snapshot.status;
      latest.active_turn_id = snapshot.activeTurnId;
      for (const entry of nativePending.values()) acceptModel(entry, snapshot, {method:'session/read',viewCursor:r.viewCursor});
    }
    const pendingAt = sessionVersions.get(rootId)??0;
    const pending = await msp.connection.request('approval/listPending', {sessionId:rootId});
    if (!Array.isArray(pending.approvals) || !Array.isArray(pending.userInputs)) throw new Error('INVALID_PENDING_INVENTORY');
    if (pendingAt === (sessionVersions.get(rootId)??0)) {
      for (const [key, entry] of pendingRequests) if (entry.view.params?.sessionId === rootId) pendingRequests.delete(key);
      for (const params of pending.approvals) pendingRequests.set(`approval:${required(params,'approvalId')}`, {view:{method:'approval/request',params}});
      for (const params of pending.userInputs) pendingRequests.set(`input:${required(params,'userInputId')}`, {view:{method:'userInput/request',params}});
    }
    latest.last_refresh = {at_ms:Date.now(),metadata_applied:fresh,viewCursor:r.viewCursor,coverage:'root_metadata_and_pending_requests'};
    changed();
    return latest.last_refresh;
  })();
  try { return await refreshInFlight; } finally { refreshInFlight = undefined; }
}

async function completeNative(entry, ack) {
  entry.ack = ack;
  if (ack.status !== 'accepted') throw new Error('UNEXPECTED_ADMISSION_STATUS');
  if (entry.kind === 'input') {
    settle(entry, {native_ack:ack,completion_condition:'native_input_admitted',requested_reasoning_effort:entry.params.reasoningEffort}, required(ack,'turnId'));
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
        details:{native_ack:ack,completion_condition:'native_configuration_applied',waiting_for:'model_readback',requested:setting.desired}});
      await refreshRoot();
      // If no matching observation exists yet, preserve accepted/pending. New
      // model work remains serialized behind it while replies/refresh continue.
    }
  } else {
    if (entry.kind === 'goal') latest.goal_admission = {action:entry.command.input.action,native_ack:ack};
    settle(entry, {native_ack:ack,completion_condition:entry.kind==='goal'?'native_goal_admitted':'native_reply_admitted'}, ack.turnId);
  }
}

async function submitNative(command, method, params, kind, setting) {
  const entry = {command,id:commandId(command),method,params,kind,setting,ack:null,resolved:false};
  nativePending.set(command.operation_id, entry);
  const ack = await msp.connection.command(method, params, {maxAttempts:1,commandId:entry.id});
  await completeNative(entry, ack);
}

async function reconcileNative(target) {
  const entry = nativePending.get(target);
  if (!entry) throw new Error('RECONCILIATION_CONTEXT_UNAVAILABLE');
  if (active.has(target)) return {target_operation_id:target,disposition:'native_request_in_progress',resubmitted:false};
  if (entry.resolved) return {target_operation_id:target,disposition:'awaiting_host_receipt',resubmitted:false};
  if (entry.ack) {
    await refreshRoot();
    return {target_operation_id:target,disposition:entry.resolved?'resolved':'pending_application',resubmitted:false};
  }
  // This is an explicit reconciliation command, not a read-only healthcheck.
  // SDK/server idempotency joins the SAME command ID with byte-equivalent params.
  try {
    const ack = await msp.connection.command(entry.method, entry.params, {maxAttempts:1,commandId:entry.id});
    await completeNative(entry, ack);
  } catch (error) {
    const rejected = error instanceof MspError && ['invalidParams','commandRejected','overloaded','backpressured'].includes(error.kind);
    saveOutcome(target, {outcome:entry.ack?'accepted':rejected?'rejected':'unknown',native_root_id:rootId,native_scope_key:nativeScope,
      details:{evidence_kind:'explicit_same_command_reconciliation',native_code:error instanceof MspError?error.code:null,native_kind:error instanceof MspError?error.kind:null}});
    throw error;
  }
  return {target_operation_id:target,disposition:entry.resolved?'resolved':'pending_application',resubmitted:true,native_command_id:entry.id};
}

async function refreshChild(id) {
  if (childReads.has(id)) return childReads.get(id);
  if (!children.has(id)) throw new Error('CHILD_OUTSIDE_OBSERVED_FAMILY');
  const pending = (async () => {
    const before = sessionVersions.get(id)??0;
    const r = await msp.connection.request('session/read', {sessionId:id,excludeItems:true});
    const snapshot = compactSession(r.session);
    if (snapshot.sessionId!==id) throw new Error('SNAPSHOT_IDENTITY_MISMATCH');
    const fresh = before===(sessionVersions.get(id)??0);
    if (fresh) children.set(id, {...children.get(id),snapshot});
    // Resume observation from the read cursor, never acquire a child's writer
    // lease or consume its result through subagent/readResult.
    await msp.connection.request('view/subscribe', {sessionId:id,after:required(r,'viewCursor')});
    const pendingAt = sessionVersions.get(id)??0;
    const questions = await msp.connection.request('approval/listPending',{sessionId:id});
    if (!Array.isArray(questions.approvals) || !Array.isArray(questions.userInputs)) throw new Error('INVALID_PENDING_INVENTORY');
    if (pendingAt===(sessionVersions.get(id)??0)) {
      for (const [key, entry] of pendingRequests) if (entry.view.params?.sessionId===id) pendingRequests.delete(key);
      for (const params of questions.approvals) pendingRequests.set(`approval:${required(params,'approvalId')}`,{view:{method:'approval/request',params}});
      for (const params of questions.userInputs) pendingRequests.set(`input:${required(params,'userInputId')}`,{view:{method:'userInput/request',params}});
    }
    const refreshed={at_ms:Date.now(),session_id:id,metadata_applied:fresh,viewCursor:r.viewCursor,coverage:'observed_child_metadata_and_pending_requests'};
    children.set(id,{...children.get(id),last_refresh:refreshed});changed();
    return refreshed;
  })();
  childReads.set(id,pending);
  try {return await pending;} finally {childReads.delete(id);}
}
async function followChild(id) {
  try {await refreshChild(id);} catch {latest.gaps++;changed();}
}
async function startNative(command) {
  if (handshake) throw new Error('NATIVE_ALREADY_OWNED');
  const options=command.route.native_options;
  required(options,'workspaceRoot'); required(options,'modelId'); required(options,'reasoningEffort');
  if (!path.isAbsolute(options.workspaceRoot)) throw new Error('WORKSPACE_MUST_BE_ABSOLUTE');
  const allowed=['workspaceRoot','modelId','providerId','reasoningEffort','approvalMode'];
  for (const name of Object.keys(options)) if (!allowed.includes(name)) throw new Error(`UNSUPPORTED_NATIVE_OPTION_${name}`);
  Object.assign(routeDefaults, options);
  // Explicit launch only, never from describe/status/reconnect. Current vendor auth is inherited.
  handshake=spawnMspConnection({command:config.command,args:config.args,cwd:options.workspaceRoot,
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
    const key=p.approvalId?`approval:${required(p,'approvalId')}`:`input:${required(p,'userInputId')}`;
    pendingRequests.set(key,{view:{method:request.method,params:p}}); changed();
    return {};
  });
  handshake.exited.then(exit=>{nativeReady=false;latest.execution='native_exited';latest.exit=exit;changed();},()=>{nativeReady=false;latest.execution='native_failed';changed();});
  msp=await handshake.initialize({clientInfo:{name:'eliot-swarm-controller',version:'0.1.0'}});
  const init=msp.initializeResult;
  const home=await realpath(required(init,'museHome'));
  nativeScope=`muse:${process.platform}:${process.platform==='win32'?home.toLowerCase():home}`;
  latest.server=init.serverInfo; latest.schema=init.schema;
  latest.fingerprint_warning=Boolean(msp.fingerprintWarning);
  const params={workspaceRoot:options.workspaceRoot,modelId:options.modelId};
  if(options.providerId!==undefined)params.providerId=options.providerId;
  if(options.approvalMode!==undefined)params.approvalMode=options.approvalMode;
  const r=await msp.connection.command('session/start',params,{maxAttempts:1,commandId:commandId(command)});
  rootId=required(r.session,'sessionId'); latest.session=compactSession(r.session);latest.execution=r.session.status;
  if(r.session.modelId!==options.modelId)throw new Error('MODEL_READBACK_MISMATCH');
  if(options.providerId!==undefined && r.session.providerId!==options.providerId)throw new Error('PROVIDER_READBACK_MISMATCH');
  latest.requested_reasoning_effort=options.reasoningEffort;
  latest.reasoning_application='explicit_per_turn_option'; // Sampled at submission, not a claimed inference measurement.
  nativeReady=true;changed();
  return {native_scope_key:nativeScope,native_root_id:rootId,details:{session:latest.session,server:init.serverInfo,
    reasoning:{requested:options.reasoningEffort,application:'per_turn_at_submission'},fingerprint_warning:Boolean(msp.fingerprintWarning)}};
}
async function execute(command) {
  const base={operation_id:command.operation_id};
  let nativeAdmissionPossible=false;
  try {
    let result;
    if (command.method==='agent.open') { nativeAdmissionPossible=true;result=await startNative(command); }
    else {
      if (!msp || command.native_root_id!==rootId) throw new Error('NATIVE_IDENTITY_MISMATCH');
      const p=command.input;
      if (command.method==='task.dispatch' || command.method==='agent.send') {
        const observed=await msp.connection.request('session/read',{sessionId:rootId});
        if(!modelMatches(observed.session,routeDefaults))throw new Error('MODEL_CHANGED_BEFORE_SEND');
        const steer=p.delivery==='steer';
        const input={sessionId:rootId,input:[{type:'text',text:required(p,'text')}],reasoningEffort:routeDefaults.reasoningEffort};
        if(command.method === 'task.dispatch') input.input.unshift({type:'text',text:'Task specification: '+JSON.stringify(p.task_snapshot)});
        if(steer)input.expectedTurnId=required(p,'expected_turn_id');else input.ifBusy='queue';
        nativeAdmissionPossible=true;
        await submitNative(command,steer?'turn/steer':'turn/start',input,'input');
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
        if (goal.startsWork) {
          if (standingEffort !== routeDefaults.reasoningEffort) throw new Error('CONFIGURE_STANDING_EFFORT_BEFORE_GOAL');
          const read = await msp.connection.request('session/read',{sessionId:rootId});
          if (!modelMatches(read.session,routeDefaults)) throw new Error('MODEL_CHANGED_BEFORE_GOAL');
        }
        nativeAdmissionPossible = true;
        await submitNative(command,goal.method,goal.params,'goal');
        return;
      } else if (command.method === 'agent.refresh') {
        const target = p.session_id===undefined?rootId:required(p,'session_id');
        result = {details:{completion_condition:'native_read_completed',snapshot:await (target===rootId?refreshRoot():refreshChild(target))}};
      } else if (command.method === 'agent.reconcile') {
        nativeAdmissionPossible = true;
        result = {details:await reconcileNative(required(p,'operation_id'))};
      } else throw new Error('UNSUPPORTED_OPERATION');
      result.native_root_id=rootId;result.native_scope_key=nativeScope;
    }
    saveOutcome(command.operation_id,{outcome:'applied',...result});
  } catch(error) {
    // Only protocol rejection proves non-admission. Transport/parsing/spawn failures may have effects.
    const rejected=!nativeAdmissionPossible || error instanceof MspError && ['invalidParams','commandRejected','overloaded','backpressured'].includes(error.kind);
    const accepted = nativePending.get(command.operation_id)?.ack;
    const outcome={...base,outcome:accepted?'accepted':rejected?'rejected':'unknown',details:{error_type:error.name,native_kind:error instanceof MspError?error.kind:null,native_code:error instanceof MspError?error.code:null,diagnostic_code:error instanceof MspError?'NATIVE_ERROR':String(error.message).slice(0,120)}};
    if(rootId){outcome.native_root_id=rootId;outcome.native_scope_key=nativeScope;}
    saveOutcome(command.operation_id,outcome);
  } finally {
    active.delete(command.operation_id);
    if(nativePending.get(command.operation_id)?.hostAcknowledged)nativePending.delete(command.operation_id);
    changed();
  }
}
async function report(link = control) {
  for(const [id,outcome] of outcomes){
    await link.call('module.outcome',outcome);
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
    const state=observation();
    if(Buffer.byteLength(JSON.stringify(state))>700000){
      // Keep pending protocol requests and mark incomplete inventory rather than discard outcomes.
      state.observed_children=state.observed_children.slice(-100);state.gaps++;state.family_completeness='partial';
    }
    await link.call('module.observe',{event_id:`${bootId}:${at}`,sequence:at,state});lastSentRevision=at;
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
  // Only explicit termination of this SDK owner closes the native transport.
  if(handshake)await handshake.close().catch(()=>{});
}
process.once('SIGINT',()=>{void stop();});process.once('SIGTERM',()=>{void stop();});
while(!stopping){
  try{
    control=new Control(config.endpoint,credential);await control.connect();
    await control.call('module.hello',{boot_id:bootId,module_artifact_id:config.moduleArtifactId,native_ready:nativeReady,
      ...(rootId?{native_root_id:rootId,native_scope_key:nativeScope}:{})});
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
          if(['agent.reply','agent.refresh','agent.reconcile'].includes(command.method)) void execute(command);
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
