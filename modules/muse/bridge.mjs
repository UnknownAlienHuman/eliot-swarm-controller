#!/usr/bin/env node
// SDK-owned native execution. Host IPC reconnect never closes Muse or repeats input.
import { readFile, realpath } from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { spawnMspConnection, MspError } from '@muse-code/sdk';
import { Control } from './control.mjs';

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
    modelId:s.modelId, providerId:s.providerId, attention:s.attention, turnCount:s.turnCount };
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
  if (n.method === 'view/gap' || n.method === 'session/viewHealthChanged') {
    latest.gaps++; latest.view_health = { method:n.method, sessionId:p.sessionId, status:p.status ?? p.health ?? 'unknown' };
  }
  if (n.method === 'turn/started' || n.method === 'turn/completed') {
    const state = { sessionId:p.sessionId, turnId:p.turnId, commandId:p.commandId,
      event:n.method, terminal:p.terminal, viewCursor:p.viewCursor };
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
  if (item?.childSessionId) {
    if (children.has(item.childSessionId) || children.size < 2000) {
      const old = children.get(item.childSessionId);
      if (!old || item.itemId !== old.itemId || Number(item.revision ?? 0) >= Number(old.revision ?? 0)) {
        children.set(item.childSessionId, { sessionId:item.childSessionId, parentSessionId:p.sessionId,
          itemId:item.itemId, parentTurnId:item.turnId, status:item.status, controlStatus:item.controlStatus, revision:item.revision });
      }
      if (!old && msp) {
        // Subscribe from an observed read cursor; never acquire a child's writer lease.
        void followChild(item.childSessionId);
      }
    } else latest.gaps++;
  }
  if (n.method === 'session/goalChanged' && p.sessionId === rootId) latest.goal = p.goal ?? null;
  if (n.method === 'session/reasoningEffortChanged' && p.sessionId === rootId) latest.reasoning_effort = p.reasoningEffort;
  if (n.method === 'usage/changed') latest.usage = { basis:'native_snapshot', value:p };
  if (n.method === 'session/modelChanged' && p.sessionId === rootId) latest.model = { modelId:p.modelId,providerId:p.providerId };
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
async function followChild(id) {
  try {
    const r = await msp.connection.request('session/read', {sessionId:id});
    const child=compactSession(r.session);
    children.set(id, {...children.get(id), snapshot:child}); changed();
    await msp.connection.request('view/subscribe', {sessionId:id,after:required(r,'viewCursor')});
  } catch { latest.gaps++; changed(); }
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
  const id=commandId(command);
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
        if(observed.session?.modelId!==routeDefaults.modelId)throw new Error('MODEL_CHANGED_BEFORE_SEND');
        const steer=p.delivery==='steer';
        const input={sessionId:rootId,input:[{type:'text',text:required(p,'text')}],reasoningEffort:routeDefaults.reasoningEffort};
        if(command.method === 'task.dispatch') input.input.unshift({type:'text',text:'Task specification: '+JSON.stringify(p.task_snapshot)});
        if(steer)input.expectedTurnId=required(p,'expected_turn_id');else input.ifBusy='queue';
        nativeAdmissionPossible=true;
        const r=await msp.connection.command(steer?'turn/steer':'turn/start',input,{maxAttempts:1,commandId:id});
        if(r.status!=='accepted')throw new Error('UNEXPECTED_ADMISSION_STATUS');
        result={turn_id:required(r,'turnId'),details:{native_ack:r,completion_condition:'native_input_admitted',requested_reasoning_effort:routeDefaults.reasoningEffort}};
      } else if(command.method==='agent.reply') {
        const r=p.reply;
        const method=required(r,'method');
        if(!['approval/decide','userInput/answer','userInput/cancel','userInput/clarify'].includes(method))throw new Error('UNSUPPORTED_REPLY_METHOD');
        const params=r.params;
        if(!params || typeof params!=='object' || !(params.sessionId===rootId || children.has(params.sessionId)))throw new Error('REPLY_OUTSIDE_OBSERVED_FAMILY');
        nativeAdmissionPossible=true;
        const ack=await msp.connection.command(method,params,{maxAttempts:1,commandId:id});
        result={details:{native_ack:ack,completion_condition:'native_reply_admitted'}};
      } else throw new Error('UNSUPPORTED_OPERATION');
      result.native_root_id=rootId;result.native_scope_key=nativeScope;
    }
    outcomes.set(command.operation_id,{...base,outcome:'applied',...result});
  } catch(error) {
    // Only protocol rejection proves non-admission. Transport/parsing/spawn failures may have effects.
    const rejected=!nativeAdmissionPossible || error instanceof MspError && ['invalidParams','commandRejected','overloaded','backpressured'].includes(error.kind);
    const outcome={...base,outcome:rejected?'rejected':'unknown',details:{error_type:error.name,native_kind:error instanceof MspError?error.kind:null,native_code:error instanceof MspError?error.code:null,diagnostic_code:error instanceof MspError?'NATIVE_ERROR':String(error.message).slice(0,120)}};
    if(rootId){outcome.native_root_id=rootId;outcome.native_scope_key=nativeScope;}
    outcomes.set(command.operation_id,outcome);
  } finally { active.delete(command.operation_id); changed(); }
}
async function report(link = control) {
  for(const [id,outcome] of outcomes){await link.call('module.outcome',outcome);outcomes.delete(id);}
  if(lastSentRevision!==revision){
    const at=revision;
    const state=observation();
    if(Buffer.byteLength(JSON.stringify(state))>700000){
      // Keep pending protocol requests and mark incomplete inventory rather than discard outcomes.
      state.observed_children=state.observed_children.slice(-100);state.gaps++;state.family_completeness='partial';
    }
    await link.call('module.observe',{event_id:`${bootId}:${at}`,state});lastSentRevision=at;
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
          if(command.method==='agent.reply') void execute(command);
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
