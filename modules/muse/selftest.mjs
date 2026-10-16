#!/usr/bin/env node
// Fixture self-test for the Muse bridge's recorded-observation derivations
// and checkpoint round-trip (program section 15 / R18). No native
// installed native executable, account or model call is involved: fixtures are authored
// from the pinned SDK sources and MSP schema (see fixtures/*.json
// provenance comments). The live bridge behaviors these pin are
// submitNative/reconcileNative and child refresh in bridge.mjs; the derivations themselves
// live in observe.mjs so they can be exercised here without a host.
// Run:  node selftest.mjs
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import net from 'node:net';
import { createHash, randomUUID } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import {
  commandIdFor, durabilityProfile, gapFillObservation, hostDeathObservation,
  failedReconcileOutcome,
} from './observe.mjs';
import { setTimeout as delay } from 'node:timers/promises';
import { nativeMcpServers, startNativeMcpSession } from './settings.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
async function load(name) {
  return JSON.parse(await readFile(path.join(here, 'fixtures', name), 'utf8'));
}
const AT_MS = 1780000000123;
const museDescriptor = JSON.parse(await readFile(path.join(here, 'module-descriptor.template.json'), 'utf8'));
const compareText = (left,right) => left<right?-1:left>right?1:0;
function fixtureSchemaClaims(schemas) {
  return schemas.map(schema => ({schema_id:schema.schema_id,version:schema.version,
    ...(typeof schema.sha256==='string'?{sha256:schema.sha256}:{})})).sort((left,right)=>
      compareText(left.schema_id,right.schema_id)||compareText(left.version,right.version)
        ||compareText(left.sha256??'',right.sha256??''));
}
const museModuleContractClaim = {
  schema_version:1,
  module_id:museDescriptor.module_id,
  artifact:{artifact_id:museDescriptor.artifact.artifact_id,version:museDescriptor.artifact.version,
    ...(typeof museDescriptor.artifact.build_id==='string'?{build_id:museDescriptor.artifact.build_id}:{})},
  protocol:{major:1,minor:0},
  capabilities:[...museDescriptor.capabilities].sort(compareText),
  config_schema:museDescriptor.config_schema??null,
  ...(museDescriptor.pre_input_open==null?{}:{pre_input_open:museDescriptor.pre_input_open}),
  command_schemas:fixtureSchemaClaims(museDescriptor.command_schemas),
  event_schemas:fixtureSchemaClaims(museDescriptor.event_schemas),
};
const museModuleContractNegotiation = {
  status:'negotiated',
  source:'store_registered_descriptor',
  descriptor_revision:1,
  module_id:museModuleContractClaim.module_id,
  artifact:museModuleContractClaim.artifact,
  protocol:museModuleContractClaim.protocol,
  capabilities:museModuleContractClaim.capabilities,
  config_schema:museModuleContractClaim.config_schema,
  pre_input_open:museModuleContractClaim.pre_input_open??null,
  command_schemas:museModuleContractClaim.command_schemas,
  event_schemas:museModuleContractClaim.event_schemas,
  effects_authorized_by_descriptor:false,
};
function canonicalFixtureJson(value) {
  if(Array.isArray(value))return `[${value.map(canonicalFixtureJson).join(',')}]`;
  if(value!==null&&typeof value==='object')return `{${Object.keys(value).sort(compareText)
    .map(key=>`${JSON.stringify(key)}:${canonicalFixtureJson(value[key])}`).join(',')}}`;
  return JSON.stringify(value);
}

// 1. Durability profile: three distinct recorded states; an unrecognized
// handshake value never collapses into durable (SS2.13.1).
{
  const fixture = await load('durability.json');
  for (const c of fixture.cases) {
    assert.deepEqual(durabilityProfile(c.initialize_result), c.expected, c.name);
  }
  const unrecognized = fixture.cases.filter(c => c.expected.profile === 'unrecognized');
  assert.ok(unrecognized.length >= 2, 'fixture covers unrecognized values');
  for (const c of unrecognized) {
    assert.notEqual(durabilityProfile(c.initialize_result).profile, 'durable', c.name);
  }
  console.log(`PASS durability: ${fixture.cases.length} handshake readings, unrecognized never durable`);
}

// 2. Host death: ephemeral death discards the session's survival; durable
// death leaves it for explicit recovery; only exit 0 is a clean shutdown.
{
  const fixture = await load('host-death.json');
  for (const c of fixture.cases) {
    const record = hostDeathObservation(c.profile, c.exit, AT_MS);
    assert.equal(record.at_ms, AT_MS, c.name);
    assert.equal(record.profile, c.profile, c.name);
    assert.deepEqual(record.exit,
      c.exit ? { code:c.exit.code, signal:c.exit.signal } : null, c.name);
    assert.equal(record.exit_kind, c.expected.exit_kind, c.name);
    assert.equal(record.abnormal, c.expected.abnormal, c.name);
    assert.equal(record.session_survives, c.expected.session_survives, c.name);
  }
  console.log(`PASS host-death: ${fixture.cases.length} death records incl. ephemeral host death`);
}

// 3. View gaps: each bracket is recorded verbatim as an unfilled hole.
// Coalesced and overlapping brackets are never merged or ordered (cursors
// are opaque, SS4.1); a foreign session's gap stays under its own ID.
{
  const fixture = await load('view-gap.json');
  const records = fixture.cases.map(c => gapFillObservation(c.params, AT_MS));
  fixture.cases.forEach((c, i) => {
    const record = records[i];
    assert.equal(record.status, c.expected.status, c.name);
    assert.equal(record.reason, 'splice_fill_not_available_on_compact_observation_path', c.name);
    assert.equal(record.session_id, c.expected.session_id, c.name);
    assert.equal(record.after, c.expected.after, `${c.name}: after relayed byte-exact`);
    assert.equal(record.next, c.expected.next, `${c.name}: next relayed byte-exact`);
    assert.equal(record.cursors_complete, c.expected.cursors_complete, c.name);
    assert.equal(record.at_ms, AT_MS, c.name);
    // No derived ordering or merged range is ever attached to a record.
    assert.deepEqual(Object.keys(record).sort(),
      ['after','at_ms','cursors_complete','next','reason','session_id','status'], c.name);
  });
  const foreign = records[fixture.cases.findIndex(c => c.name.startsWith('foreign'))];
  assert.notEqual(foreign.session_id, fixture.root_session, 'foreign gap not attributed to root');
  console.log(`PASS view-gap: ${fixture.cases.length} verbatim unfilled records incl. overlap and foreign session`);
}

// 4. Command identity and checkpoint round-trip: an entry persisted
// before native I/O survives bridge loss with everything explicit
// same-ID reconciliation needs, and its ID re-derives from the recorded
// command facts (disconnect after write/before ACK; same-ID replay).
{
  const fixture = await load('checkpoint-roundtrip.json');
  const [operationId, entry] = fixture.state.native_pending[0];
  assert.equal(commandIdFor(entry.command.operation_id, entry.command.created_at_ms), entry.id,
    'persisted ID re-derives from the recorded command facts');
  assert.equal(commandIdFor(entry.command.operation_id, entry.command.created_at_ms),
    commandIdFor(entry.command.operation_id, entry.command.created_at_ms), 'minting is deterministic');
  assert.match(entry.id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    'native ID is UUIDv7-shaped');

  const dir = await mkdtemp(path.join(os.tmpdir(), 'muse-selftest-'));
  try {
    await writeFile(path.join(dir, 'owner.json'),
      JSON.stringify({ version:1, process:{ purpose:'module' }, token:'fixture-boot-token' }));
    process.env.ELIOT_SWARM_MODULE_STATE = dir;
    process.env.ELIOT_SWARM_MODULE_OWNER = path.join(dir, 'owner.json');
    const { recoveryState } = await import('./checkpoint.mjs');
    const writer = await recoveryState();
    assert.equal(writer.saved, undefined, 'fresh state dir has no checkpoint');
    const checkpointFile=path.join(dir,'checkpoint.json');
    await writeFile(checkpointFile,'null','utf8');
    await assert.rejects(recoveryState(),error=>error.message==='CHECKPOINT_CORRUPT',
      'a present JSON null checkpoint is corrupt, not an absent checkpoint');
    await rm(checkpointFile);
    const missingAgain=await recoveryState();
    assert.equal(missingAgain.saved,undefined,'removing the corrupt checkpoint restores the existing missing-file behavior');
    await writer.write(fixture.state);
    const reader = await recoveryState();
    assert.equal(reader.saved.version, 1);
    assert.deepEqual(reader.saved.native_pending, fixture.state.native_pending,
      'native_pending survives the round-trip byte-for-byte as JSON');
    assert.deepEqual(reader.saved.outcomes, fixture.state.outcomes);
    assert.equal(reader.saved.root_id, fixture.state.root_id);
    assert.equal(reader.saved.native_scope, fixture.state.native_scope);
    const [restoredId, restored] = reader.saved.native_pending[0];
    assert.equal(restoredId, operationId);
    for (const field of fixture.reconcile_fields) {
      assert.ok(Object.hasOwn(restored, field), `restored entry carries ${field} for same-ID reconcile`);
    }
    assert.equal(restored.resolved, false, 'lost reply leaves the command unresolved, not settled');
    assert.equal(restored.ack, null, 'no ACK was recorded before the reply was lost');
    assert.deepEqual(restored.params, entry.params, 'replay params are the persisted bytes');
  } finally {
    delete process.env.ELIOT_SWARM_MODULE_STATE;
    delete process.env.ELIOT_SWARM_MODULE_OWNER;
    await rm(dir, { recursive:true, force:true });
  }
  console.log('PASS checkpoint: pending command round-trips with ID, payload and reconcile fields');
}

// 4a. Startup recovery must reject a present null checkpoint at the real bridge
// consumer before connecting to the host or launching the configured native.
{
  const dir=await mkdtemp(path.join(os.tmpdir(),'muse-null-checkpoint-startup-'));
  const stateDir=path.join(dir,'state');
  await mkdir(stateDir);
  const ownerFile=path.join(stateDir,'owner.json');
  const token='fixture-null-checkpoint-boot';
  await writeFile(ownerFile,JSON.stringify({version:1,process:{purpose:'module'},token}));
  await writeFile(path.join(stateDir,'checkpoint.json'),'null','utf8');
  const credentialFile=path.join(dir,'credential.json');
  const configFile=path.join(dir,'module.json');
  const nativeScript=path.join(dir,'native-marker.mjs');
  const nativeMarker=path.join(dir,'native-started');
  const endpoint=process.platform==='win32'?'\\\\.\\pipe\\muse-null-startup-'+randomUUID():path.join(dir,'control.sock');
  let controlConnections=0;
  const server=net.createServer(socket=>{controlConnections++;socket.destroy();});
  let bridge;
  let stderr='';
  try {
    await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(endpoint,resolve);});
    await writeFile(credentialFile,JSON.stringify({client_id:'muse-null-fixture',token:'fixture-only'}));
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.13',
      command:process.execPath,args:[nativeScript]}));
    await writeFile(nativeScript,
      `import { writeFile } from 'node:fs/promises';\nawait writeFile(${JSON.stringify(nativeMarker)},'started');\n`,'utf8');
    const env={PATH:process.env.PATH,ELIOT_SWARM_MODULE_STATE:stateDir,ELIOT_SWARM_MODULE_OWNER:ownerFile,
      TEMP:os.tmpdir(),TMP:os.tmpdir(),SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR};
    bridge=spawn(process.execPath,[path.join(here,'bridge.mjs'),'--config',configFile],
      {cwd:here,env,windowsHide:true,stdio:['ignore','ignore','pipe']});
    bridge.stderr.setEncoding('utf8');
    bridge.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4000);});
    const closed=new Promise(resolve=>bridge.once('close',(code,signal)=>resolve({code,signal})));
    let outcome=await Promise.race([closed,delay(5000).then(()=>null)]);
    if(!outcome){bridge.kill();outcome=await closed;}
    assert.notEqual(outcome.code,0,'bridge startup refuses the corrupt checkpoint');
    assert.match(stderr,/CHECKPOINT_CORRUPT/,'bridge exposes the existing recovery-gap classification');
    assert.equal(controlConnections,0,'corrupt recovery stops before host connection');
    await assert.rejects(readFile(nativeMarker),error=>error.code==='ENOENT',
      'corrupt recovery stops before configured native launch');
    console.log('PASS checkpoint startup fixture: null is a recovery gap before host/native startup');
  } finally {
    if(bridge&&bridge.exitCode===null)bridge.kill();
    if(bridge&&bridge.exitCode===null)await Promise.race([new Promise(resolve=>bridge.once('close',resolve)),delay(3000)]);
    if(server.listening)await new Promise(resolve=>server.close(resolve));
    await rm(dir,{recursive:true,force:true});
  }
}

// 5. Failed-reconcile classification follows the pinned SDK's durable
// commandRejected settlement signals; nothing-admitted validation,
// overload and backpressure errors remain unknown (review section 13).
{
  const fixture = await load('reconcile-outcome.json');
  for (const c of fixture.cases) {
    assert.equal(failedReconcileOutcome(c.native_error, c.has_ack), c.expected, c.name);
  }
  console.log(`PASS reconcile-outcome: ${fixture.cases.length} pinned-SDK settlement classifications`);
}

// 6. Exercise the real bridge callbacks against a bounded local fixture host
// and a fake native MSP process. This covers restored-child freshness,
// method-not-found server requests, and native exact-turn stale rejection; it
// never starts Muse or invokes a model/provider.
{
  const childFixture=await load('child-freshness.json');
  const unknownFixture=await load('unknown-server-request.json');
  const oversizedPendingRequestFixture=await load('oversized-pending-request.json');
  const oversizedViewGapFixture=await load('oversized-view-gap.json');
  const staleFixture=await load('exact-turn-stale.json');
  const checkpointFixture=await load('checkpoint-roundtrip.json');
  const dir=await mkdtemp(path.join(os.tmpdir(),'muse-bridge-selftest-'));
  const stateDir=path.join(dir,'state');
  const home=path.join(dir,'native-home');
  await mkdir(stateDir);await mkdir(home);
  // A prior bridge launch persists realpath(museHome), which can be a Windows
  // 8.3 path even though route_defaults retains the long path.
  const canonicalHome=await realpath(home);
  const ownerFile=path.join(stateDir,'owner.json');
  const token='fixture-reboot-boot';
  await writeFile(ownerFile,JSON.stringify({version:1,process:{purpose:'module'},token}));
  const nativeScope=`muse:${process.platform}:${process.platform==='win32'?canonicalHome.toLowerCase():canonicalHome}`;
  const saved=structuredClone(checkpointFixture.state);
  // Build a fresh current-artifact fixture; the historical roundtrip sample
  // itself remains byte-identical and must never be cross-served at runtime.
  saved.module_artifact_id=museDescriptor.artifact.artifact_id;
  saved.native_scope=nativeScope;
  saved.route_defaults.workspaceRoot=home;
  saved.route_defaults.modelId=childFixture.root_session.modelId;
  saved.route_defaults.providerId=childFixture.root_session.providerId;
  saved.latest={execution:'recovery_required',family_completeness:'partial',gaps:1,observed_children:[],pending_requests:[]};
  saved.children=[[childFixture.checkpoint_child.sessionId,childFixture.checkpoint_child],
    [childFixture.unverified_child.sessionId,childFixture.unverified_child]];
  await writeFile(path.join(stateDir,'checkpoint.json'),JSON.stringify({version:1,...saved}));
  function storeCommand(operationId,createdAtMs,method,input) {
    // These fake module.next rows model the public RuntimeCommand identity:
    // digest the original, un-enriched request and retain the authenticated
    // binding plus the exact descriptor artifact selected for this route.
    const inputSha256=createHash('sha256').update(canonicalFixtureJson(input),'utf8').digest('hex');
    return {operation_id:operationId,created_at_ms:createdAtMs,method,binding_id:'fixture-binding',generation:1,
      native_root_id:saved.root_id,route:{runtime:'muse',module_artifact_id:museDescriptor.artifact.artifact_id},
      input_sha256:inputSha256,input};
  }

  const nativeFixturePath=path.join(dir,'native-fixtures.json');
  const auditPath=path.join(dir,'native-audit.json');
  const approvalRequest=(identity,rawArgsBytes=0)=>({jsonrpc:'2.0',id:identity.request_id,
    method:oversizedPendingRequestFixture.expected.method,params:{
      approvalId:identity.approval_id,
      availableChoices:[{choiceId:'allow-once',decision:'approved',label:'Allow once',scope:'once'}],
      currentRequirementId:{approvalId:identity.approval_id,sourceIndex:0},
      itemId:'itm_oversized_fixture',judgeEscalated:false,protectedWrite:false,
      rawArgs:'x'.repeat(rawArgsBytes),sessionId:identity.session_id,
      sourceRange:{first:{id:'event_fixture',sequence:1},last:{id:'event_fixture',sequence:1},
        stream:{id:identity.session_id,kind:'session'}},
      subject:{kind:'tool',toolName:'fixture'},taskId:'task_oversized_fixture',
      toolCallId:'toolcall_oversized_fixture',toolName:'fixture',turnId:'turn_oversized_fixture',
      viewCursor:'cursor_oversized_fixture',
    }});
  const oversizedPendingRequest=approvalRequest(oversizedPendingRequestFixture,
    oversizedPendingRequestFixture.payload_bytes);
  const identityOverflowRequest=approvalRequest({
    request_id:oversizedPendingRequestFixture.identity_overflow.request_id,
    session_id:oversizedPendingRequestFixture.identity_overflow.session_id,
    approval_id:'x'.repeat(oversizedPendingRequestFixture.identity_overflow.approval_id_bytes),
  });
  const fittingSiblingRequest=approvalRequest({
    request_id:oversizedPendingRequestFixture.fitting_sibling.request_id,
    session_id:oversizedPendingRequestFixture.fitting_sibling.session_id,
    approval_id:oversizedPendingRequestFixture.fitting_sibling.approval_id,
  });
  const opaqueCursor=(prefix,byteLength,fill)=>{
    if(prefix.length>byteLength||Buffer.byteLength(prefix)!==prefix.length)throw new Error('INVALID_GAP_CURSOR_FIXTURE');
    return prefix+fill.repeat(byteLength-prefix.length);
  };
  const oversizedViewGap={jsonrpc:'2.0',method:'view/gap',params:{
    after:opaqueCursor(oversizedViewGapFixture.after_prefix,oversizedViewGapFixture.after_ascii_bytes,'a'),
    next:opaqueCursor(oversizedViewGapFixture.next_prefix,oversizedViewGapFixture.next_ascii_bytes,'n'),
    sessionId:oversizedViewGapFixture.session_id,
  }};
  const oversizedViewGapBytes=Buffer.byteLength(JSON.stringify(oversizedViewGap))+1;
  assert.ok(oversizedViewGapBytes>oversizedViewGapFixture.outgoing_frame_limit_bytes,
    'the native gap bracket exceeds the outbound observation envelope');
  assert.ok(oversizedViewGapBytes<oversizedViewGapFixture.incoming_frame_limit_bytes,
    'the valid native view/gap notification remains below the pinned SDK inbound frame cap');
  const pendingRequestIds=[oversizedPendingRequest.id,identityOverflowRequest.id,fittingSiblingRequest.id];
  assert.ok(Buffer.byteLength(JSON.stringify(oversizedPendingRequest))>1_048_576,
    'synthetic approval/request details exceed the frame cap before projection');
  assert.ok(Buffer.byteLength(JSON.stringify(identityOverflowRequest.params.approvalId))>1_048_576,
    'the second exact native identity cannot fit in any bounded observation frame');
  await writeFile(nativeFixturePath,JSON.stringify({child:childFixture,unknown:unknownFixture,stale:staleFixture,
    oversizedPendingRequest,identityOverflowRequest,fittingSiblingRequest,oversizedViewGap,pendingRequestIds}));
  const nativeScript=path.join(dir,'fake-native.mjs');
  await writeFile(nativeScript,String.raw`
import { readFile, writeFile } from 'node:fs/promises';
const arg=name=>{const i=process.argv.indexOf(name);return i<0?null:process.argv[i+1]};
const home=arg('--home'),auditPath=arg('--audit'),fixturePath=arg('--fixtures');
const fixtures=JSON.parse(await readFile(fixturePath,'utf8'));
const audit={methods:[],child_reads:0,unknown_response:null,pending_request_responses:[],turn_steer:null,finished:false};
let auditTail=Promise.resolve(),buffer='',rootReads=0,finishAfterPendingAcks=false;
const save=()=>{const bytes=JSON.stringify(audit);auditTail=auditTail.then(()=>writeFile(auditPath,bytes));return auditTail};
const send=packet=>process.stdout.write(JSON.stringify(packet)+'\n');
const respond=(id,result,error,after)=>process.stdout.write(JSON.stringify({jsonrpc:'2.0',id,...(error?{error}:{result})})+'\n',after);
async function finishIfReady(){
  if(!finishAfterPendingAcks||audit.pending_request_responses.length!==fixtures.pendingRequestIds.length)return;
  audit.finished=true;await save();process.exit(0);
}
function sessionRead(id){
  if(id===fixtures.child.root_session.sessionId){
    rootReads++;
    return rootReads>=3?{...fixtures.child.root_session,status:'running',activeTurnId:fixtures.stale.native.active_turn_id}:fixtures.child.root_session;
  }
  if(id===fixtures.child.child_session.sessionId)return fixtures.child.child_session;
  return null;
}
function handle(frame){
  if(typeof frame.method!=='string'){
    if(frame.id===fixtures.unknown.request.id){audit.unknown_response=frame;void save();}
    if(fixtures.pendingRequestIds.includes(frame.id)){
      audit.pending_request_responses.push(frame);void save();void finishIfReady();
    }
    return;
  }
  if(!Object.hasOwn(frame,'id')){
    if(frame.method==='initialized'){
      send(fixtures.unknown.request);
      const replay=fixtures.child.equal_revision_replay;
      send({jsonrpc:'2.0',method:'item/updated',params:{sessionId:fixtures.child.root_session.sessionId,item:{kind:'subagent',childSessionId:fixtures.child.checkpoint_child.sessionId,itemId:replay.itemId,subagentId:'subagent_historical',turnId:'turn_historical',revision:replay.revision,status:replay.status,controlStatus:replay.controlStatus,result:replay.result}}});
    }
    return;
  }
  audit.methods.push(frame.method);
  if(frame.method==='initialize'){
    if(!/^[a-z0-9_]+$/.test(frame.params?.clientInfo?.name??'')){
      respond(frame.id,null,{code:-32602,message:'clientInfo.name must match ^[a-z0-9_]+$'});return;
    }
    respond(frame.id,{museHome:home,serverInfo:{name:'fixture-muse',version:'1'},schema:{fingerprint:'fixture-schema'}});return;
  }
  if(frame.method==='session/read'){
    const id=frame.params?.sessionId;
    if(id===fixtures.child.checkpoint_child.sessionId){
      audit.child_reads++;
      if(audit.child_reads===1){respond(frame.id,null,fixtures.child.first_child_read_error);void save();return;}
    }
    const session=sessionRead(id);
    if(!session){respond(frame.id,null,{code:-32004,message:'Fixture session is missing',data:{kind:'notFound'}});return;}
    respond(frame.id,{session,viewCursor:id===fixtures.child.checkpoint_child.sessionId?'cursor_child_current_fixture':'cursor_root_fixture'});return;
  }
  if(frame.method==='session/resume'){
    respond(frame.id,{session:fixtures.child.root_session,viewCursor:'cursor_resume_fixture'});return;
  }
  if(frame.method==='approval/listPending'){respond(frame.id,{approvals:[],userInputs:[]});return;}
  if(frame.method==='view/subscribe'){respond(frame.id,{});return;}
  if(frame.method==='turn/steer'){
    audit.turn_steer={expectedTurnId:frame.params?.expectedTurnId,commandId:frame.params?.commandId};
    send({jsonrpc:'2.0',method:fixtures.child.native_event_after_fresh.method,params:fixtures.child.native_event_after_fresh.params});
    send(fixtures.oversizedViewGap);
    // Deliver after the refresh read. The first overlarge request is observed
    // before the identity-overflow siblings arrive, proving projection does
    // not mutate the bridge's full pending-request inventory.
    send(fixtures.oversizedPendingRequest);
    setTimeout(()=>send(fixtures.identityOverflowRequest),1200);
    setTimeout(()=>send(fixtures.fittingSiblingRequest),1600);
    audit.child_event_after_fresh=true;
    if(frame.params?.expectedTurnId!==fixtures.stale.native.active_turn_id){
      respond(frame.id,null,fixtures.stale.native.stale_error,()=>{void save();});
    } else respond(frame.id,{status:'accepted',turnId:fixtures.stale.native.active_turn_id});
    return;
  }
  if(frame.method==='turn/start'){
    audit.turn_start_count=(audit.turn_start_count??0)+1;
    audit.turn_start={commandId:frame.params?.commandId,text:frame.params?.input?.map(part=>part.text).join('\n')};
    if(audit.turn_start.text!==fixtures.stale.nonsettling.input.text){
      audit.unrequested_turn_start_count=(audit.unrequested_turn_start_count??0)+1;
      respond(frame.id,null,fixtures.stale.native.stale_error,()=>{void save();});
    } else respond(frame.id,null,fixtures.stale.nonsettling.native_error,async()=>{
      finishAfterPendingAcks=true;await finishIfReady();
    });
    return;
  }
  respond(frame.id,{});
}
process.stdin.setEncoding('utf8');
process.stdin.on('data',chunk=>{
  buffer+=chunk;let end;
  while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;try{handle(JSON.parse(line));}catch{process.exit(2);}}
});
process.stdin.on('end',()=>process.exit(0));
`);

  const endpoint=process.platform==='win32'?'\\\\.\\pipe\\muse-selftest-'+randomUUID():path.join(dir,'control.sock');
  const server=net.createServer();
  const sockets=new Set();
  let teardownStarted=false;
  const unexpectedSocketErrors=[];
  const commandIds={
    recover:'40000000-0000-4000-8000-000000000001',
    refresh:'40000000-0000-4000-8000-000000000002',
    stale:'40000000-0000-4000-8000-000000000003',
    nonsettling:'40000000-0000-4000-8000-000000000004',
  };
  const commands=[storeCommand(commandIds.recover,AT_MS,'agent.recover',{expected_boot_id:token})];
  const outcomes=new Map(),observations=[],rpcMethods=[],deliveredCommands=[];
  let oversizedObservation=null,firstIdentityOnlyObservation=false,controlConnectionCount=0;
  let refreshQueued=false,staleQueued=false,nonsettlingQueued=false,resolveFinished,rejectFinished;
  const finished=new Promise((resolve,reject)=>{resolveFinished=resolve;rejectFinished=reject;});
  function maybeAdvance() {
    const state=observations.at(-1);
    const child=state?.observed_children?.find(c=>c.sessionId===childFixture.checkpoint_child.sessionId);
    const recoveryOutcome=outcomes.get(commandIds.recover);
    if(recoveryOutcome&&recoveryOutcome.outcome!=='applied'){
      rejectFinished(new Error(`agent.recover did not apply: ${JSON.stringify(recoveryOutcome)}`));
      return;
    }
    const recoveryReady=recoveryOutcome?.outcome==='applied'&&state?.recovery?.status==='resumed';
    if(recoveryReady&&child?.last_refresh_attempt?.status==='failed'&&!refreshQueued){
      refreshQueued=true;commands.push(storeCommand(commandIds.refresh,AT_MS+1,'agent.refresh',
        {session_id:childFixture.checkpoint_child.sessionId}));
    }
    if(outcomes.has(commandIds.refresh)&&child?.snapshot_freshness==='fresh'&&!staleQueued){
      staleQueued=true;commands.push(storeCommand(commandIds.stale,AT_MS+2,'agent.send',staleFixture.input));
    }
    if(outcomes.has(commandIds.stale)&&child?.snapshot_freshness==='stale'
        &&child?.snapshot_freshness_reason===childFixture.expected.after_event_snapshot_freshness_reason
        &&!outcomes.has(commandIds.nonsettling)&&!nonsettlingQueued){
      nonsettlingQueued=true;commands.push(storeCommand(commandIds.nonsettling,AT_MS+3,'agent.send',
        staleFixture.nonsettling.input));
    }
    if(outcomes.has(commandIds.nonsettling)&&oversizedObservation)resolveFinished();
  }
  async function respondHost(socket,packet,frameBytes){
    rpcMethods.push(packet.method);
    let result={};
    if(packet.method==='client.hello')result={client_id:'muse-fixture-client'};
    else if(packet.method==='module.hello'){
      assert.deepEqual(packet.params.module_contract,museModuleContractClaim);
      result={binding_id:'fixture-binding',generation:1,recovery_required:true,
        native_root_id:saved.root_id,native_scope_key:saved.native_scope,
        module_contract_negotiation:museModuleContractNegotiation};
    }
    else if(packet.method==='module.next'){
      if(commands.length){result={command:commands.shift()};deliveredCommands.push(result.command.method);}
      else await delay(10);
    } else if(packet.method==='module.outcome'){
      const outcome=packet.params;outcomes.set(outcome.operation_id,outcome);maybeAdvance();
    } else if(packet.method==='module.observe'){
      const state=packet.params.state;
      observations.push(state);
      const gap=state.gap_fill;
      const hasOversizedViewGap=gap?.status==='unfilled'
        &&gap?.session_id===oversizedViewGapFixture.session_id
        &&gap?.projection_state==='incomplete'
        &&gap?.projection_reason==='opaque_gap_bracket_did_not_fit_complete_observation_frame'
        &&gap?.projection_omission==='opaque_cursor_bracket'
        &&gap?.cursors_complete===false&&gap?.after===null&&gap?.next===null;
      const pending=Array.isArray(state.pending_requests)?state.pending_requests:[];
      const hasOversizedIdentity=pending.some(request=>request.method===oversizedPendingRequestFixture.expected.method
        &&request.params?.sessionId===oversizedPendingRequestFixture.session_id
        &&request.params?.approvalId===oversizedPendingRequestFixture.approval_id);
      const hasFittingSibling=pending.some(request=>request.method===oversizedPendingRequestFixture.expected.method
        &&request.params?.sessionId===oversizedPendingRequestFixture.fitting_sibling.session_id
        &&request.params?.approvalId===oversizedPendingRequestFixture.fitting_sibling.approval_id);
      if(hasOversizedIdentity&&!hasFittingSibling)firstIdentityOnlyObservation=true;
      if(hasOversizedIdentity&&hasFittingSibling&&hasOversizedViewGap)oversizedObservation={state,frameBytes};
      maybeAdvance();
    }
    socket.write(JSON.stringify({jsonrpc:'2.0',id:packet.id,result})+'\n');
  }
  server.on('connection',socket=>{
    controlConnectionCount++;
    sockets.add(socket);socket.setEncoding('utf8');let buffer='';
    socket.on('close',()=>sockets.delete(socket));
    socket.on('error',error=>{
      if(teardownStarted&&error?.code==='ECONNRESET')return;
      const diagnostic={code:error?.code??null,message:String(error?.message??error)};
      unexpectedSocketErrors.push(diagnostic);
      rejectFinished(new Error(`unexpected fixture host socket error: ${diagnostic.code??'unknown'}: ${diagnostic.message}`));
    });
    socket.on('data',chunk=>{
      buffer+=chunk;let end;
      while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
        let packet;try{packet=JSON.parse(line);}catch{rejectFinished(new Error('fixture host received invalid JSON'));return;}
        void respondHost(socket,packet,Buffer.byteLength(line)).catch(rejectFinished);
      }
    });
  });

  let bridge;
  let stderr='';
  try {
    await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(endpoint,resolve);});
    const credentialFile=path.join(dir,'credential.json');
    const configFile=path.join(dir,'module.json');
    await writeFile(credentialFile,JSON.stringify({client_id:'muse-fixture-client',token:'fixture-only'}));
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.13',
      command:process.execPath,args:[nativeScript,'--home',home,'--audit',auditPath,'--fixtures',nativeFixturePath]}));
    const env={PATH:process.env.PATH,ELIOT_SWARM_MODULE_STATE:stateDir,ELIOT_SWARM_MODULE_OWNER:ownerFile,
      TEMP:os.tmpdir(),TMP:os.tmpdir(),SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR};
    bridge=spawn(process.execPath,[path.join(here,'bridge.mjs'),'--config',configFile],{cwd:here,env,windowsHide:true,stdio:['ignore','ignore','pipe']});
    bridge.stderr.setEncoding('utf8');bridge.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4000);});
    const deadline=Date.now()+20000;
    while(Date.now()<deadline){
      if(bridge.exitCode!==null)throw new Error(`fixture bridge exited early (${bridge.exitCode}): ${stderr}`);
      const result=await Promise.race([finished.then(()=>true),delay(25).then(()=>false)]);
      if(result)break;
    }
    let nativeDebug;
    try{nativeDebug=JSON.parse(await readFile(auditPath,'utf8'));}catch{}
    const debug={delivered:deliveredCommands,
      outcomes:[...outcomes.values()].map(({operation_id,outcome,details})=>({operation_id,outcome,details})),
      observed:observations.slice(-4).map(state=>`${state.execution}:${(state.observed_children??[]).map(child=>`${child.sessionId}/${child.snapshot_freshness}/${child.last_refresh_attempt?.status}`).join(',')}`),
      rpc:[...new Set(rpcMethods)],native:nativeDebug,
      oversized_observation:oversizedObservation?{frame_bytes:oversizedObservation.frameBytes,
        pending_requests:oversizedObservation.state.pending_requests,
        pending_requests_state:oversizedObservation.state.pending_requests_state,
        pending_requests_count:oversizedObservation.state.pending_requests_count,
        pending_requests_omitted_count:oversizedObservation.state.pending_requests_omitted_count,
        gap_fill:oversizedObservation.state.gap_fill}:null,
      control_connections:controlConnectionCount};
    assert.ok(outcomes.has(commandIds.stale),`bridge did not complete stale-turn fixture: ${stderr}; ${JSON.stringify(debug)}`);
    const auditDeadline=Date.now()+3000;
    let audit;
    while(Date.now()<auditDeadline){
      try{audit=JSON.parse(await readFile(auditPath,'utf8'));}catch{}
      if(audit?.finished)break;
      await delay(25);
    }
    assert.ok(audit?.finished,`fake native fixture did not finish cleanly: ${JSON.stringify({
      delivered:deliveredCommands,outcomes:[...outcomes.values()].map(x=>({operation_id:x.operation_id,outcome:x.outcome})),
      native_methods:audit?.methods,pending_response_ids:audit?.pending_request_responses?.map(x=>x.id),
      native_finished:audit?.finished,oversized_observation:oversizedObservation!==null,
      projection:observations.at(-1)?.observation_projection_state,
      gap:observations.at(-1)?.gap_fill?.projection_state,
      children:observations.at(-1)?.observed_children?.map(x=>({session_id:x.sessionId,freshness:x.snapshot_freshness,reason:x.snapshot_freshness_reason})),
      stderr,
    })}`);
    assert.ok(oversizedObservation,
      'the connected host receives fitting request siblings with a truthful oversized-gap projection');
    assert.equal(firstIdentityOnlyObservation,true,
      'the earlier projection preserves its identity in a later report after sibling overflow arrives');
    assert.ok(oversizedObservation.frameBytes<=1_048_576,
      'the complete serialized module.observe JSON-RPC line stays within the control frame cap');
    const projectedGap=oversizedObservation.state.gap_fill;
    assert.equal(projectedGap.status,oversizedViewGapFixture.expected.projected_gap.status);
    assert.equal(projectedGap.reason,oversizedViewGapFixture.expected.projected_gap.reason);
    assert.equal(projectedGap.session_id,oversizedViewGapFixture.expected.projected_gap.session_id);
    assert.equal(projectedGap.after,null,'an opaque oversized cursor is omitted whole, never truncated');
    assert.equal(projectedGap.next,null,'both cursors are omitted together');
    assert.equal(projectedGap.cursors_complete,false);
    assert.equal(projectedGap.projection_state,'incomplete');
    assert.equal(projectedGap.projection_reason,
      oversizedViewGapFixture.expected.projected_gap.projection_reason);
    assert.equal(projectedGap.projection_omission,
      oversizedViewGapFixture.expected.projected_gap.projection_omission);
    const pendingRequests=oversizedObservation.state.pending_requests;
    assert.ok(Array.isArray(pendingRequests),'overflow preserves an array of individually addressable pending requests');
    const oversizedDetails=pendingRequests.find(request=>request.params?.sessionId===oversizedPendingRequestFixture.session_id
      &&request.params?.approvalId===oversizedPendingRequestFixture.approval_id);
    const fittingSibling=pendingRequests.find(request=>request.params?.sessionId===oversizedPendingRequestFixture.fitting_sibling.session_id
      &&request.params?.approvalId===oversizedPendingRequestFixture.fitting_sibling.approval_id);
    assert.ok(oversizedDetails,'large request details do not erase that request identity');
    assert.ok(fittingSibling,'an oversized sibling identity does not suppress a later fitting request');
    for(const expected of oversizedViewGapFixture.expected.fitting_pending_requests){
      assert.ok(pendingRequests.some(request=>request.method===expected.method
        &&request.params?.sessionId===expected.params.sessionId
        &&request.params?.approvalId===expected.params.approvalId),
      `connected gap projection retains exact Store-addressable request ${expected.params.approvalId}`);
    }
    for(const request of [oversizedDetails,fittingSibling]){
      assert.equal(request.method,oversizedPendingRequestFixture.expected.method);
      assert.equal(request.request_details_state,oversizedPendingRequestFixture.expected.request_details_state);
      assert.equal(request.params.rawArgs,undefined,'omitted request details are not presented as complete');
    }
    assert.equal(pendingRequests.some(request=>request.params?.approvalId===identityOverflowRequest.params.approvalId),false,
      'the unrepresentable native identity is not truncated or fabricated');
    assert.equal(oversizedObservation.state.pending_requests_state,'partial');
    assert.equal(oversizedObservation.state.pending_requests_count,oversizedPendingRequestFixture.expected.pending_request_count);
    assert.equal(oversizedObservation.state.pending_requests_omitted_count,
      oversizedPendingRequestFixture.expected.omitted_identity_count);
    assert.match(oversizedObservation.state.pending_requests_identity_sha256,/^[0-9a-f]{64}$/,
      'omitted pending identities remain represented by an inventory digest');
    const expectedIdentityDigest=createHash('sha256');
    for(const request of [oversizedPendingRequest,identityOverflowRequest,fittingSiblingRequest]){
      expectedIdentityDigest.update(JSON.stringify([request.method,request.params.sessionId,request.params.approvalId]));
      expectedIdentityDigest.update('\n');
    }
    assert.equal(oversizedObservation.state.pending_requests_identity_sha256,expectedIdentityDigest.digest('hex'),
      'the gap digest covers exact native identities, including the omitted oversized ID');
    assert.ok(oversizedObservation.state.gaps>=1&&oversizedObservation.state.family_completeness==='partial',
      'omission remains an explicit partial observation gap');
    assert.equal(controlConnectionCount,1,'oversized pending identities do not force a reconnect loop');
    assert.equal(audit.pending_request_responses.length,pendingRequestIds.length,
      'all three native server requests receive a response');
    for(const response of audit.pending_request_responses){
      assert.deepEqual(response.result,oversizedPendingRequestFixture.expected.presentation_ack,
        'request presentation ACK does not decide or settle the native question');
    }
    assert.equal(audit.methods.some(method=>method==='approval/decide'||method==='userInput/answer'),false,
      'projection never sends a native decision');
    const staleOutcome=outcomes.get(commandIds.stale);
    assert.equal(staleOutcome.outcome,staleFixture.expected.operation_outcome,'native stale rejection is preserved as rejected');
    assert.equal(staleOutcome.details.native_kind,staleFixture.expected.native_kind,'settlement comes from native commandRejected');
    assert.deepEqual(audit.turn_steer?.expectedTurnId,staleFixture.expected.expectedTurnId,'bridge forwards exact expected turn');
    const nonsettlingOutcome=outcomes.get(commandIds.nonsettling);
    assert.equal(nonsettlingOutcome.outcome,staleFixture.nonsettling.expected_outcome,'invalidParams after native submission remains unknown');
    assert.equal(nonsettlingOutcome.details.native_kind,staleFixture.nonsettling.expected_kind);
    assert.equal(audit.unrequested_turn_start_count??0,staleFixture.expected.turn_start_count,'stale steer never falls back to turn/start');
    assert.equal(audit.turn_start?.commandId,commandIdFor(commandIds.nonsettling,AT_MS+3),'only the separate next-turn Operation reaches turn/start');
    const retainedCheckpoint=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
    assert.ok(retainedCheckpoint.native_pending.some(([id])=>id===commandIds.nonsettling),'non-settling native error retains its pending same-ID reconciliation record');
    assert.equal(audit.unknown_response?.error?.code,unknownFixture.expected.error.code,'unknown native request receives method-not-found');
    assert.equal(audit.unknown_response?.error?.data?.kind,unknownFixture.expected.error.data.kind);
    const firstState=observations[0];
    const firstChild=firstState?.observed_children?.find(c=>c.sessionId===childFixture.checkpoint_child.sessionId);
    assert.equal(firstChild?.snapshot_freshness,childFixture.expected.restored_snapshot_freshness,'restored child snapshot starts stale');
    const unverifiedChild=firstState?.observed_children?.find(c=>c.sessionId===childFixture.unverified_child.sessionId);
    assert.equal(unverifiedChild?.snapshot_freshness,childFixture.expected.unverified_snapshot_freshness,'checkpoint child without an exact snapshot starts unknown');
    const afterFailed=observations.find(state=>state.observed_children?.some(c=>c.sessionId===childFixture.checkpoint_child.sessionId&&c.last_refresh_attempt?.status==='failed'));
    const failedChild=afterFailed?.observed_children?.find(c=>c.sessionId===childFixture.checkpoint_child.sessionId);
    assert.equal(failedChild?.snapshot_freshness,childFixture.expected.failed_read_snapshot_freshness,'missing native child read retains stale evidence');
    assert.equal(failedChild?.snapshot?.status,childFixture.expected.retained_snapshot_status_after_failed_read,'failed read retains the previous snapshot');
    assert.equal(failedChild?.last_refresh?.viewCursor,childFixture.checkpoint_child.last_refresh.viewCursor,'failed read retains last successful refresh evidence');
    const finalState=[...observations].reverse().find(state=>state.observed_children?.some(c=>c.sessionId===childFixture.checkpoint_child.sessionId&&c.snapshot_freshness==='fresh'));
    const finalChild=finalState?.observed_children?.find(c=>c.sessionId===childFixture.checkpoint_child.sessionId);
    assert.equal(finalChild?.snapshot_freshness,childFixture.expected.stable_read_snapshot_freshness,'stable exact child read becomes fresh');
    assert.equal(finalChild?.snapshot?.status,childFixture.expected.stable_snapshot_status);
    assert.equal(finalChild?.itemId,childFixture.expected.itemId,'old exact child item identity survives recovery');
    assert.equal(finalChild?.subagentId,childFixture.expected.subagentId,'old exact subagent identity survives recovery');
    assert.equal(finalChild?.status,childFixture.expected.status_after_equal_revision_replay,'equal-revision historical replay cannot overwrite child status');
    assert.equal(finalState?.session?.status,childFixture.expected.parent_status,'idle parent is observed as idle');
    assert.notEqual(finalChild?.status,'completed','parent idle does not synthesize child completion');
    const afterNativeEvent=[...observations].reverse().find(state=>state.observed_children?.some(c=>c.sessionId===childFixture.checkpoint_child.sessionId
      &&c.snapshot_freshness_reason===childFixture.expected.after_event_snapshot_freshness_reason));
    const invalidatedChild=afterNativeEvent?.observed_children?.find(c=>c.sessionId===childFixture.checkpoint_child.sessionId);
    assert.equal(invalidatedChild?.snapshot_freshness,childFixture.expected.after_event_snapshot_freshness,'a later native event invalidates the current marker');
    assert.equal(audit.child_event_after_fresh,true,'the fixture native host emitted a child event after the stable read');
    console.log('PASS bridge fixtures: restored-child freshness, retained stale evidence, unknown server request, exact-turn stale rejection, bounded pending-request observation');
  } finally {
    teardownStarted=true;
    if(bridge&&bridge.exitCode===null)bridge.kill();
    if(bridge&&bridge.exitCode===null)await Promise.race([new Promise(resolve=>bridge.once('exit',resolve)),delay(3000)]);
    for(const socket of sockets)socket.destroy();
    if(server.listening)await new Promise(resolve=>server.close(resolve));
    await rm(dir,{recursive:true,force:true});
    assert.deepEqual(unexpectedSocketErrors,[],`unexpected fixture socket errors: ${JSON.stringify(unexpectedSocketErrors)}`);
  }
}

// 7. An ephemeral host with no compatible metadata readback must reject
// dispatch before creating a native turn or a replay record.
{
  const fixture=await load('session-list-unsupported.json');
  const dir=await mkdtemp(path.join(os.tmpdir(),'muse-session-list-unsupported-'));
  const stateDir=path.join(dir,'state');
  const home=path.join(dir,'native-home');
  await mkdir(stateDir);await mkdir(home);
  const ownerFile=path.join(stateDir,'owner.json');
  const token='fixture-session-list-unsupported-boot';
  await writeFile(ownerFile,JSON.stringify({version:1,process:{purpose:'module'},token}));
  const nativeScript=path.join(dir,'fake-native.mjs');
  const fixturePath=path.join(dir,'native-fixture.json');
  const auditPath=path.join(dir,'native-audit.json');
  await writeFile(fixturePath,JSON.stringify(fixture));
  const sessionId='ses_list_unsupported_fixture';
  const openId='50000000-0000-4000-8000-000000000001';
  const refreshId='50000000-0000-4000-8000-000000000002';
  const sendId='50000000-0000-4000-8000-000000000003';
  const sentText='fixture-only-payload-must-not-reach-native';
  function command(operationId,createdAtMs,method,input,routeExtra={}) {
    const inputSha256=createHash('sha256').update(canonicalFixtureJson(input),'utf8').digest('hex');
    return {operation_id:operationId,created_at_ms:createdAtMs,method,binding_id:'fixture-binding',generation:1,
      native_root_id:sessionId,route:{runtime:'muse',module_artifact_id:museDescriptor.artifact.artifact_id,...routeExtra},
      input_sha256:inputSha256,input};
  }
  const openCommand=command(openId,AT_MS+20,'agent.open',{},
    {native_options:{workspaceRoot:home,modelId:'fixture-model',reasoningEffort:'max'}});
  const refreshCommand=command(refreshId,AT_MS+21,'agent.refresh',{});
  const sendCommand=command(sendId,AT_MS+22,'agent.send',{delivery:'next_turn',text:sentText});
  await writeFile(nativeScript,String.raw`
import { readFile, writeFile } from 'node:fs/promises';
const arg=name=>{const i=process.argv.indexOf(name);return i<0?null:process.argv[i+1]};
const home=arg('--home'),auditPath=arg('--audit'),fixturePath=arg('--fixtures');
const fixture=JSON.parse(await readFile(fixturePath,'utf8'));
const audit={methods:[],turn_start_count:0,turn_start_text:null,session_read_count:0,session_resume_count:0,
  session_list_count:0,session_list_params:null};
const session={sessionId:${JSON.stringify(sessionId)},status:'idle',activeTurnId:null,modelId:'fixture-model',
  providerId:null,turnCount:0,workspaceRoot:home};
const save=()=>writeFile(auditPath,JSON.stringify(audit));
const respond=(id,result,error,after)=>process.stdout.write(JSON.stringify({jsonrpc:'2.0',id,...(error?{error}:{result})})+'\n',after);
async function handle(frame){
  if(typeof frame.method!=='string'||!Object.hasOwn(frame,'id'))return;
  audit.methods.push(frame.method);
  if(frame.method==='initialize'){
    await save();
    respond(frame.id,{museHome:home,serverInfo:{name:'fixture-muse',version:'1.4.0'},schema:{fingerprint:'fixture-schema'},
      sessionDurability:'ephemeral',grantedCapabilities:[]});
    return;
  }
  if(frame.method==='usage/read'){
    await save();respond(frame.id,null,{code:-32601,message:'Method not found',data:{kind:'methodNotFound'}});return;
  }
  if(frame.method==='session/start'){
    await save();respond(frame.id,{session});return;
  }
  if(frame.method==='session/list'){
    audit.session_list_count++;
    audit.session_list_params=frame.params;
    await save();
    if(audit.session_list_count===1){respond(frame.id,{sessions:[session],nextCursor:null});return;}
    respond(frame.id,null,{code:fixture.native_error.code,message:'Method not found',data:fixture.native_error.data},()=>process.exit(0));return;
  }
  if(frame.method==='approval/listPending'){await save();respond(frame.id,{approvals:[],userInputs:[]});return;}
  if(frame.method==='session/read')audit.session_read_count++;
  if(frame.method==='session/resume')audit.session_resume_count++;
  if(frame.method==='turn/start'){
    audit.turn_start_count++;
    audit.turn_start_text=frame.params?.input?.map(part=>part.text).join('\n')??null;
  }
  await save();respond(frame.id,{});
}
process.stdin.setEncoding('utf8');
let buffer='';
process.stdin.on('data',chunk=>{
  buffer+=chunk;let end;
  while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
    try{void handle(JSON.parse(line)).catch(()=>process.exit(2));}catch{process.exit(2);}}
});
process.stdin.on('end',()=>process.exit(0));
`);
  const endpoint=process.platform==='win32'?'\\\\.\\pipe\\muse-session-list-unsupported-'+randomUUID():path.join(dir,'control.sock');
  const sockets=new Set();
  const outcomes=new Map();
  const commands=[openCommand];
  let refreshQueued=false,sendQueued=false,bridge,stderr='',teardownStarted=false;
  let resolveFinished,rejectFinished;
  const finished=new Promise((resolve,reject)=>{resolveFinished=resolve;rejectFinished=reject;});
  const server=net.createServer(socket=>{
    sockets.add(socket);socket.setEncoding('utf8');let buffer='';
    socket.on('close',()=>sockets.delete(socket));
    socket.on('error',error=>{if(teardownStarted&&error?.code==='ECONNRESET')return;
      rejectFinished(new Error(`session-list fixture socket: ${error?.code??error?.message??error}`));});
    socket.on('data',chunk=>{
      buffer+=chunk;let end;
      while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
        let packet;try{packet=JSON.parse(line);}catch{rejectFinished(new Error('session-list fixture received invalid JSON'));return;}
        void respondHost(socket,packet).catch(rejectFinished);
      }
    });
  });
  async function respondHost(socket,packet) {
    let result={};
    if(packet.method==='client.hello')result={client_id:'muse-fixture-client'};
    else if(packet.method==='module.hello'){
      assert.deepEqual(packet.params.module_contract,museModuleContractClaim);
      result={binding_id:'fixture-binding',generation:1,recovery_required:false,native_root_id:null,native_scope_key:null,
        module_contract_negotiation:museModuleContractNegotiation};
    } else if(packet.method==='module.next') {
      if(commands.length)result={command:commands.shift()};
      else await delay(10);
    } else if(packet.method==='module.outcome') {
      const outcome=packet.params;outcomes.set(outcome.operation_id,outcome);
      if(outcome.operation_id===openId&&outcome.outcome==='applied'&&!refreshQueued){commands.push(refreshCommand);refreshQueued=true;}
      if(outcome.operation_id===refreshId&&outcome.outcome==='applied'&&!sendQueued){commands.push(sendCommand);sendQueued=true;}
      if(outcome.operation_id===sendId)resolveFinished(outcome);
    }
    socket.write(JSON.stringify({jsonrpc:'2.0',id:packet.id,result})+'\n');
  }
  let closed,closeResult;
  try {
    await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(endpoint,resolve);});
    const credentialFile=path.join(dir,'credential.json');
    const configFile=path.join(dir,'module.json');
    await writeFile(credentialFile,JSON.stringify({client_id:'muse-fixture-client',token:'fixture-only'}));
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.13',
      command:process.execPath,args:[nativeScript,'--home',home,'--audit',auditPath,'--fixtures',fixturePath]}));
    const env={PATH:process.env.PATH,ELIOT_SWARM_MODULE_STATE:stateDir,ELIOT_SWARM_MODULE_OWNER:ownerFile,
      TEMP:os.tmpdir(),TMP:os.tmpdir(),SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR};
    bridge=spawn(process.execPath,[path.join(here,'bridge.mjs'),'--config',configFile],
      {cwd:here,env,windowsHide:true,stdio:['ignore','ignore','pipe']});
    bridge.stderr.setEncoding('utf8');bridge.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4000);});
    closed=new Promise(resolve=>bridge.once('close',(code,signal)=>resolve({code,signal})));
    const outcome=await Promise.race([finished,delay(12000).then(()=>{throw new Error(`session-list fixture timed out: ${stderr}`);})]);
    assert.equal(outcomes.get(openId)?.outcome,'applied','the fixture root opens before dispatch preflight');
    assert.equal(outcomes.get(refreshId)?.outcome,'applied','an exact ephemeral session/list row supports refresh');
    assert.equal(outcome.outcome,fixture.expected.operation_outcome);
    assert.equal(outcome.details.diagnostic_code,fixture.expected.diagnostic);
    assert.equal(outcome.details.native_code,fixture.native_error.code);
    assert.equal(outcome.details.native_kind,fixture.native_error.data.kind);
  } finally {
    teardownStarted=true;
    if(bridge&&bridge.exitCode===null)bridge.kill('SIGTERM');
    if(bridge&&closed)closeResult=await Promise.race([closed,delay(5000).then(()=>null)]);
    for(const socket of sockets)socket.destroy();
    if(server.listening)await new Promise(resolve=>server.close(resolve));
    try {
      const audit=JSON.parse(await readFile(auditPath,'utf8'));
      for(const method of fixture.expected.no_requests)assert.equal(audit.methods.includes(method),false,`${method} stays before no native effect`);
      assert.equal(audit.turn_start_count,0,'unsupported readback prevents turn/start');
      assert.equal(audit.turn_start_text,null,'fixture payload never reaches native input');
      assert.equal(audit.session_list_count,2,'one supported row is read, then the unavailable dispatch readback fails');
      assert.equal(audit.session_read_count,0,'ephemeral profile never calls the withheld session/read method');
      assert.equal(audit.session_resume_count,0,'dispatch failure never attempts recovery');
      assert.equal(audit.session_list_params?.workspaceRoot,home,'readback stays within the configured workspace');
      assert.equal(audit.session_list_params?.limit,200,'the native list page is bounded');
      const checkpoint=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
      assert.equal(checkpoint.native_pending?.some(([id])=>id===sendId),false,'failed preflight creates no replayable native command');
    } finally {
      await rm(dir,{recursive:true,force:true});
    }
    assert.ok(closeResult,'the fixture bridge stops after the fake native host has exited');
  }
  console.log('PASS ephemeral session-list fixture: unsupported readback rejects before native input or replay record');
}

// 8. Work-starting commands keep the exact session snapshot current across
// preflight and durable submission. These are actual bridge subprocesses
// against an account-free local MSP fixture, never a live model/session.
async function runWorkFenceCase(scenario) {
  const dir=await mkdtemp(path.join(os.tmpdir(),`muse-work-fence-${scenario}-`));
  const stateDir=path.join(dir,'state');
  const home=path.join(dir,'native-home');
  await mkdir(stateDir);await mkdir(home);
  const canonicalHome=await realpath(home);
  const nativeScope=`muse:${process.platform}:${process.platform==='win32'?canonicalHome.toLowerCase():canonicalHome}`;
  const ownerFile=path.join(stateDir,'owner.json');
  const ownerToken=`fixture-work-fence-${scenario}`;
  await writeFile(ownerFile,JSON.stringify({version:1,process:{purpose:'module'},token:ownerToken}));
  const nativeScript=path.join(dir,'fake-native.mjs');
  const gateScript=path.join(dir,'checkpoint-gate.mjs');
  const fixturePath=path.join(dir,'native-fixture.json');
  const auditPath=path.join(dir,'native-audit.json');
  const nativeFramesPath=path.join(dir,'native-frames.ndjson');
  const gateSignal=path.join(dir,'checkpoint-durable.signal');
  const gateRelease=path.join(dir,'checkpoint-gate.release');
  const sessionId=`ses_work_fence_${scenario}`;
  const identities={baseline:['000000000001','000000000002','000000000003'],
    'read-race':['000000000011','000000000013','000000000012'],
    'checkpoint-race':['000000000021','000000000022','000000000023'],
    'goal-checkpoint-race':['000000000031','000000000033','000000000032'],
    'pending-configure-recovery':['000000000041','000000000042','000000000043'],
    'native-effort-default':['000000000061','000000000062','000000000063'],
    'explicit-effort-mismatch':['000000000071','000000000072','000000000073']}[scenario];
  if(!identities)throw new Error('UNKNOWN_WORK_FENCE_FIXTURE');
  const isEffortMismatch=scenario==='explicit-effort-mismatch';
  const isEffortCase=scenario==='native-effort-default'||isEffortMismatch;
  const isGoal=scenario==='read-race'||scenario==='goal-checkpoint-race'||isEffortCase;
  const isCheckpointRace=scenario==='checkpoint-race'||scenario==='goal-checkpoint-race';
  const isPendingConfigureRecovery=scenario==='pending-configure-recovery';
  const openId=`60000000-0000-4000-8000-${identities[0]}`;
  const workId=`60000000-0000-4000-8000-${identities[1]}`;
  const configureId=`60000000-0000-4000-8000-${identities[2]}`;
  const reconcileId='60000000-0000-4000-8000-000000000023';
  const openAt=AT_MS+100,effortAt=AT_MS+101,configureAt=AT_MS+102,workAt=AT_MS+103,reconcileAt=AT_MS+104;
  const operationId=n=>`60000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
  const effortId=operationId(52);
  const sentText=`fixture-only-${scenario}-payload`;
  function command(operationId,createdAtMs,method,input,routeExtra={}) {
    const inputSha256=createHash('sha256').update(canonicalFixtureJson(input),'utf8').digest('hex');
    return {operation_id:operationId,created_at_ms:createdAtMs,method,binding_id:'fixture-binding',generation:1,
      native_root_id:sessionId,route:{runtime:'muse',module_artifact_id:museDescriptor.artifact.artifact_id,...routeExtra},
      input_sha256:inputSha256,input};
  }
  const openCommand=command(openId,openAt,'agent.open',{},
    {native_options:{workspaceRoot:home,modelId:'fixture-model',providerId:'fixture-provider',
      ...(scenario==='native-effort-default'?{}:{reasoningEffort:'max'})}});
  const effortCommand=command(effortId,effortAt,'agent.configure',{settings:{reasoningEffort:'max'}});
  const configureCommand=command(configureId,configureAt,'agent.configure',{settings:isPendingConfigureRecovery
    ?{model:{modelId:'fixture-model-B'}}:{reasoningEffort:'max'}});
  const workCommand=isGoal
    ?command(workId,workAt,'agent.goal',{action:'set',objective:isEffortCase
      ?'fixture goal using native standing effort':'fixture goal must stay fenced'})
    :command(workId,workAt,'agent.send',{delivery:'next_turn',text:sentText});
  const effortGoalCommands=isEffortCase?[workCommand,
    command(operationId(81),workAt+1,'agent.goal',{action:'edit',objective:'fixture edited goal using native standing effort'}),
    command(operationId(82),workAt+2,'agent.goal',{action:'resume'})]:[];
  const reconcileCommand=command(reconcileId,reconcileAt,'agent.reconcile',{operation_id:workId});
  const recoveryAt=AT_MS+200;
  const recoveryId=operationId(44),dispatchId=operationId(45),goalId=operationId(46),replyId=operationId(47);
  const refreshId=operationId(48),reconcilePendingId=operationId(49),reconcileResolvedId=operationId(50);
  const postReconcileWorkId=operationId(51);
  const recoveryCommand=command(recoveryId,recoveryAt,'agent.recover',{expected_boot_id:ownerToken});
  const dispatchPrompt='fixture dispatch prompt after configure reconciliation';
  const dispatchSource='fixture task source after configure reconciliation';
  const dispatchTaskId='task_fixture_configure_fence',dispatchAttemptId='attempt_fixture_configure_fence';
  const dispatchSnapshotSha256='a'.repeat(64);
  const dispatchCommand=command(dispatchId,recoveryAt+1,'task.dispatch',{
    text:dispatchSource,
    task_prompt:{schema_id:'swarm.task_prompt',schema_version:1,task_id:dispatchTaskId,task_revision:1,
      attempt_id:dispatchAttemptId,
      task_snapshot_sha256:dispatchSnapshotSha256,
      prompt_sha256:createHash('sha256').update(dispatchPrompt,'utf8').digest('hex'),
      prompt_bytes:Buffer.byteLength(dispatchPrompt,'utf8'),prompt:dispatchPrompt},
    task_dispatch_context:{schema_version:1,operation_id:dispatchId,binding_id:'fixture-binding',
      binding_generation:1,worker_boot_id:ownerToken,attempt_id:dispatchAttemptId,task_id:dispatchTaskId,
      task_revision:1,task_snapshot_sha256:dispatchSnapshotSha256,
      source_text_sha256:createHash('sha256').update(dispatchSource,'utf8').digest('hex'),
      source_text_bytes:Buffer.byteLength(dispatchSource,'utf8')},
  });
  const goalCommand=command(goalId,recoveryAt+2,'agent.goal',{action:'set',objective:'fixture goal after configure reconciliation'});
  const replyCommand=command(replyId,recoveryAt+3,'agent.reply',{reply:{method:'approval/decide',params:{
    sessionId,approvalId:'approval_fixture_configure_fence',decision:'approved',choiceId:'allow-once'}}});
  const refreshCommand=command(refreshId,recoveryAt+4,'agent.refresh',{});
  const reconcilePendingCommand=command(reconcilePendingId,recoveryAt+5,'agent.reconcile',{operation_id:configureId});
  const reconcileResolvedCommand=command(reconcileResolvedId,recoveryAt+6,'agent.reconcile',{operation_id:configureId});
  const postReconcileWorkCommand=command(postReconcileWorkId,recoveryAt+7,'agent.send',
    {delivery:'next_turn',text:'fixture work after exact configure reconciliation'});
  const selectedModelPath=path.join(home,'selected-model.json');
  const fixture={scenario,sessionId,home,auditPath,nativeFramesPath,gateSignal,gateRelease,workId,
    nativeEffortNotification:isEffortCase?'high':null,
    workMethod:workCommand.method,workText:sentText};
  await writeFile(fixturePath,JSON.stringify(fixture));
  await writeFile(nativeScript,String.raw`
import { access, appendFile, readFile, writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
const arg=name=>{const i=process.argv.indexOf(name);return i<0?null:process.argv[i+1]};
const home=arg('--home'),auditPath=arg('--audit'),fixturePath=arg('--fixtures');
const fixture=JSON.parse(await readFile(fixturePath,'utf8'));
const nativeInstanceId=randomUUID();
let nativeFrameCount=0,nativeFrameTail=Promise.resolve();
function recordNativeFrame(frame){
  if(fixture.scenario!=='pending-configure-recovery')return Promise.resolve();
  if(typeof fixture.nativeFramesPath!=='string')throw new Error('NATIVE_FRAME_AUDIT_PATH_MISSING');
  if(++nativeFrameCount>32)throw new Error('NATIVE_FRAME_AUDIT_LIMIT');
  const record={instance_id:nativeInstanceId,pid:process.pid,jsonrpc:frame.jsonrpc,id:frame.id,method:frame.method,
    ...(frame.params===undefined?{}:{params:frame.params})};
  const line=JSON.stringify(record)+'\n';
  if(Buffer.byteLength(line,'utf8')>16384)throw new Error('NATIVE_FRAME_AUDIT_RECORD_TOO_LARGE');
  const pending=nativeFrameTail.then(()=>appendFile(fixture.nativeFramesPath,line,'utf8'));
  nativeFrameTail=pending;
  return pending;
}
const session={sessionId:fixture.sessionId,status:'idle',activeTurnId:null,modelId:'fixture-model',
  providerId:'fixture-provider',turnCount:0,workspaceRoot:home};
const auditDefaults={methods:[],session_start_params:null,session_list_count:0,session_list_params:null,
  listed_session:null,session_read_count:0,session_resume_count:0,session_set_effort_count:0,
  session_set_model_count:0,
  model_changed_notifications:0,checkpoint_gate_released:false,turn_start_count:0,turn_steer_count:0,
  effort_changed_notifications:0,effort_refresh_notifications:0,
  goal_set_count:0,goal_edit_count:0,goal_resume_count:0,turn_start_params:null,goal_set_params:null,
  goal_edit_params:null,goal_resume_params:null};
let audit=auditDefaults;
try{audit=Object.assign(auditDefaults,JSON.parse(await readFile(auditPath,'utf8')));}
catch(error){if(error?.code!=='ENOENT')throw error;}
let saveTail=Promise.resolve();
const save=()=>{
  const bytes=JSON.stringify(audit);
  const pending=saveTail.then(()=>writeFile(auditPath,bytes));
  saveTail=pending;
  return pending;
};
async function refreshFixtureSession(){
  if(fixture.scenario==='pending-configure-recovery'){
    try{Object.assign(session,JSON.parse(await readFile(path.join(home,'selected-model.json'),'utf8')));}
    catch(error){if(error?.code!=='ENOENT')throw error;}
  }
  return session;
}
const send=frame=>process.stdout.write(JSON.stringify(frame)+'\n');
const respond=(id,result,error,after)=>process.stdout.write(JSON.stringify({jsonrpc:'2.0',id,...(error?{error}:{result})})+'\n',()=>after?.());
const sourceRange=(id,sequence)=>({first:{id,sequence},last:{id,sequence},stream:{id:fixture.sessionId,kind:'session'}});
function modelChanged(){
  audit.model_changed_notifications++;
  return {jsonrpc:'2.0',method:'session/modelChanged',params:{sessionId:fixture.sessionId,
    modelId:'changed-model',providerId:'changed-provider',source:'user',sourceRange:sourceRange('fixture_model_change',2),
    viewCursor:'cursor_fixture_model_change'}};
}
function effortChanged(reasoningEffort='max'){
  const sequence=++audit.effort_changed_notifications;
  const eventId='fixture_effort_change'+(sequence===1?'':'_'+sequence);
  return {jsonrpc:'2.0',method:'session/reasoningEffortChanged',params:{sessionId:fixture.sessionId,
    reasoningEffort,source:'user',sourceRange:sourceRange(eventId,sequence),viewCursor:'cursor_'+eventId}};
}
const responseWaiters=new Map();
function waitForResponse(id){return new Promise((resolve,reject)=>{
  const timer=setTimeout(()=>{responseWaiters.delete(id);reject(new Error('CHECKPOINT_GATE_REQUEST_TIMEOUT'));},10000);
  responseWaiters.set(id,frame=>{clearTimeout(timer);resolve(frame);});
});}
async function waitForFile(filename){
  const deadline=Date.now()+10000;
  while(Date.now()<deadline){
    try{await access(filename);return;}
    catch(error){if(error?.code!=='ENOENT')throw error;await delay(1);}
  }
  throw new Error('CHECKPOINT_GATE_SIGNAL_TIMEOUT');
}
async function checkpointRace(){
  await waitForFile(fixture.gateSignal);
  send(modelChanged());
  const requestId=9001;
  const ack=waitForResponse(requestId);
  send({jsonrpc:'2.0',id:requestId,method:'approval/request',params:{
    approvalId:'approval_checkpoint_fence_fixture',
    availableChoices:[{choiceId:'allow-once',decision:'approved',label:'Allow once',scope:'once'}],
    currentRequirementId:{approvalId:'approval_checkpoint_fence_fixture',sourceIndex:0},
    itemId:'item_checkpoint_fence_fixture',judgeEscalated:false,protectedWrite:false,rawArgs:'',
    sessionId:fixture.sessionId,
    sourceRange:sourceRange('fixture_checkpoint_request',3),
    subject:{kind:'tool',toolName:'fixture'},taskId:'task_checkpoint_fence_fixture',
    toolCallId:'tool_checkpoint_fence_fixture',toolName:'fixture',turnId:'turn_checkpoint_fence_fixture',
    viewCursor:'cursor_checkpoint_fence_fixture'}});
  await ack;
  await writeFile(fixture.gateRelease,'release');
  audit.checkpoint_gate_released=true;await save();
}
async function handle(frame){
  if(typeof frame.method!=='string'){
    const resolve=responseWaiters.get(frame.id);
    if(resolve){responseWaiters.delete(frame.id);resolve(frame);}
    return;
  }
  if(fixture.scenario==='pending-configure-recovery')await recordNativeFrame(frame);
  if(!Object.hasOwn(frame,'id'))return;
  audit.methods.push(frame.method);
  if(frame.method==='initialize'){
    await save();respond(frame.id,{museHome:home,serverInfo:{name:'fixture-muse',version:'1.4.0'},
      schema:{fingerprint:'fixture-schema'},sessionDurability:fixture.scenario==='pending-configure-recovery'?'durable':'ephemeral',
      grantedCapabilities:[]});return;
  }
  if(frame.method==='usage/read'){
    await save();respond(frame.id,null,{code:-32601,message:'Method not found',data:{kind:'methodNotFound'}});return;
  }
  if(frame.method==='session/start'){
    audit.session_start_params=frame.params;await refreshFixtureSession();await save();respond(frame.id,{session});return;
  }
  if(frame.method==='session/list'){
    await refreshFixtureSession();
    audit.session_list_count++;audit.session_list_params=frame.params;
    audit.listed_session={sessionId:session.sessionId,modelId:session.modelId,providerId:session.providerId,
      status:session.status,activeTurnId:session.activeTurnId};
    // The host requests this refresh only after session/start is applied.
    // Emit once here, never during a later productive goal's preflight.
    if(fixture.nativeEffortNotification&&audit.effort_refresh_notifications===0){
      audit.effort_refresh_notifications++;
      send(effortChanged(fixture.nativeEffortNotification));
    }
    await save();
    if(fixture.scenario==='read-race'){send(modelChanged());await save();}
    const after=fixture.scenario.endsWith('checkpoint-race')?()=>{void checkpointRace().catch(()=>process.exit(2));}:undefined;
    respond(frame.id,{sessions:[session],nextCursor:null},undefined,after);return;
  }
  if(frame.method==='session/read'){
    audit.session_read_count++;await refreshFixtureSession();await save();
    respond(frame.id,{session,viewCursor:'cursor_fixture_session_read'});return;
  }
  if(frame.method==='session/resume'){
    audit.session_resume_count++;await refreshFixtureSession();await save();
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted',session,viewCursor:'cursor_fixture_resume'});return;
  }
  if(frame.method==='session/setReasoningEffort'){
    audit.session_set_effort_count++;await save();send(effortChanged());
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted'});return;
  }
  if(frame.method==='session/setModel'){
    audit.session_set_model_count++;await save();
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted'});return;
  }
  if(frame.method==='turn/start'){
    audit.turn_start_count++;audit.turn_start_params=frame.params;await save();
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted',turnId:'turn_fixture_started'});return;
  }
  if(frame.method==='turn/steer'){
    audit.turn_steer_count++;await save();
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted',turnId:'turn_fixture_steered'});return;
  }
  if(frame.method==='goal/set'){
    audit.goal_set_count++;audit.goal_set_params=frame.params;await save();
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted'});return;
  }
  if(frame.method==='goal/edit'){
    audit.goal_edit_count++;audit.goal_edit_params=frame.params;await save();
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted'});return;
  }
  if(frame.method==='goal/resume'){
    audit.goal_resume_count++;audit.goal_resume_params=frame.params;await save();
    respond(frame.id,{commandId:frame.params.commandId,status:'accepted'});return;
  }
  if(['approval/decide','userInput/answer','userInput/cancel','userInput/clarify'].includes(frame.method)){
    await save();respond(frame.id,{status:'accepted'});return;
  }
  if(frame.method==='approval/listPending'){await save();respond(frame.id,{approvals:[],userInputs:[]});return;}
  await save();respond(frame.id,{});
}
process.stdin.setEncoding('utf8');
let buffer='';
process.stdin.on('data',chunk=>{
  buffer+=chunk;let end;
  while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
    try{void handle(JSON.parse(line)).catch(()=>process.exit(2));}catch{process.exit(2);}}
});
process.stdin.on('end',()=>process.exit(0));
`);
  if(isCheckpointRace)await writeFile(gateScript,String.raw`
import { createRequire, syncBuiltinESMExports } from 'node:module';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
const require=createRequire(import.meta.url);
const fs=require('node:fs/promises');
const originalRename=fs.rename;
const checkpointPath=path.resolve(process.env.MUSE_FIXTURE_CHECKPOINT_PATH);
const targetOperation=process.env.MUSE_FIXTURE_CHECKPOINT_OPERATION;
let gateUsed=false;
fs.rename=async (from,to)=>{
  const result=await originalRename(from,to);
  if(!gateUsed&&path.resolve(String(to))===checkpointPath){
    let checkpoint;
    try{checkpoint=JSON.parse(await fs.readFile(checkpointPath,'utf8'));}catch{return result;}
    if(checkpoint.native_pending?.some(([id])=>id===targetOperation)){
      gateUsed=true;
      await fs.writeFile(process.env.MUSE_FIXTURE_GATE_SIGNAL,'durable');
      const release=process.env.MUSE_FIXTURE_GATE_RELEASE;
      const deadline=Date.now()+10000;
      while(Date.now()<deadline){
        try{await fs.access(release);return result;}
        catch(error){if(error?.code!=='ENOENT')throw error;await delay(1);}
      }
      throw new Error('CHECKPOINT_GATE_RELEASE_TIMEOUT');
    }
  }
  return result;
};
syncBuiltinESMExports();
`);
  const endpoint=process.platform==='win32'?`\\\\.\\pipe\\muse-work-fence-${scenario}-${randomUUID()}`:path.join(dir,'control.sock');
  const sockets=new Set(),outcomes=new Map(),commands=[openCommand];
  let workOutcomeCount=0,reconcileOutcomeCount=0,deferredWorkAck=null,reconcileCommandDelivered=false;
  let finishedResolve,finishedReject,configureAcceptedResolve,bridge,stderr='',teardownStarted=false,closed,closeResult;
  let moduleHelloCount=0,reconcilePendingQueued=false,pendingReplyOutcome,pendingRefreshOutcome;
  let effortGoalIndex=0,effortObservedBeforeGoals=false;
  const hostTrace=[];
  function recordHost(phase,details={}){
    if(hostTrace.length<64)hostTrace.push({phase,...details});
  }
  const fencedWorkOutcomes=new Map();
  const fencedWorkIds=new Set([workId,dispatchId,goalId]);
  let checkpointAtWorkOutcome;
  const finished=new Promise((resolve,reject)=>{finishedResolve=resolve;finishedReject=reject;});
  void finished.catch(()=>{});
  const configureAccepted=new Promise(resolve=>{configureAcceptedResolve=resolve;});
  function queuePendingModelReconcile(){
    if(isPendingConfigureRecovery&&!reconcilePendingQueued&&fencedWorkOutcomes.size===fencedWorkIds.size
        &&pendingReplyOutcome&&pendingRefreshOutcome){
      reconcilePendingQueued=true;commands.push(reconcilePendingCommand);
    }
  }
  function assertEffortCheckpoint(checkpoint){
    assert.equal(checkpoint.standing_effort,'high','the actual native notification updates standing effort');
    assert.equal(checkpoint.latest.reasoning_effort,'high');
    assert.equal(Object.hasOwn(checkpoint.route_defaults,'reasoningEffort'),isEffortMismatch,
      'an observed effort does not become a requested route override');
    assert.equal(checkpoint.route_defaults.reasoningEffort,isEffortMismatch?'max':undefined);
    assert.equal(checkpoint.latest.requested_reasoning_effort,isEffortMismatch?'max':null,
      'requested effort stays separate from native observed effort');
    assert.equal(checkpoint.latest.reasoning_application,isEffortMismatch
      ?'native_durable_default_ack_and_explicit_per_turn':'native_default_at_submission');
  }
  let primaryError;
  const server=net.createServer(socket=>{
    sockets.add(socket);socket.setEncoding('utf8');let buffer='';
    socket.on('close',()=>sockets.delete(socket));
    socket.on('error',error=>{if((teardownStarted||isPendingConfigureRecovery)&&error?.code==='ECONNRESET')return;
      recordHost('socket_error',{code:error?.code??null,message:error?.message??String(error)});
      finishedReject(new Error(`work-fence fixture socket: ${error?.code??error?.message??error}`));});
    socket.on('data',chunk=>{
      buffer+=chunk;let end;
      while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
        let packet;try{packet=JSON.parse(line);}catch{finishedReject(new Error('work-fence fixture received invalid JSON'));return;}
        void respondHost(socket,packet).catch(finishedReject);
      }
    });
  });
  function reply(socket,packet,result={}) {
    socket.write(JSON.stringify({jsonrpc:'2.0',id:packet.id,result})+'\n');
  }
  async function respondHost(socket,packet) {
    let result={};
    if(packet.method==='client.hello'){
      recordHost('client_hello');result={client_id:'muse-fixture-client'};
    }
    else if(packet.method==='module.hello'){
      assert.deepEqual(packet.params.module_contract,museModuleContractClaim);
      moduleHelloCount++;
      const recovering=isPendingConfigureRecovery&&moduleHelloCount>1;
      recordHost('module_hello',{count:moduleHelloCount,recovery_required:recovering,
        native_root_id:packet.params.native_root_id??null,native_scope_key:packet.params.native_scope_key??null});
      result={binding_id:'fixture-binding',generation:1,recovery_required:recovering,
        native_root_id:recovering?sessionId:null,
        native_scope_key:recovering?nativeScope:null,
        module_contract_negotiation:museModuleContractNegotiation};
      if(recovering)commands.push(recoveryCommand);
    } else if(packet.method==='module.next') {
      if(!commands.length)await delay(10);
      if(commands.length){
        const command=commands.shift();result={command};
        recordHost('command_delivered',{operation_id:command.operation_id,method:command.method});
        if(command.method==='agent.reconcile'){
          reconcileCommandDelivered=true;
          if(deferredWorkAck)setTimeout(()=>reply(deferredWorkAck.socket,deferredWorkAck.packet),40);
        }
      }
    } else if(packet.method==='module.outcome') {
      const outcome=packet.params;outcomes.set(outcome.operation_id,outcome);
      recordHost('outcome',{operation_id:outcome.operation_id,outcome:outcome.outcome,
        diagnostic_code:outcome.details?.diagnostic_code??null});
      if(outcome.operation_id===openId&&outcome.outcome==='applied'){
        if(isPendingConfigureRecovery)commands.push(effortCommand);
        else if(isEffortCase){
          assert.deepEqual(outcome.details.reasoning,{requested:isEffortMismatch?'max':null,
            application:isEffortMismatch?'explicit_per_turn_option':'native_default_at_submission'});
          commands.push(isEffortMismatch?configureCommand:refreshCommand);
        }
        else if(scenario==='read-race'||scenario==='goal-checkpoint-race')commands.push(configureCommand,workCommand);
        else commands.push(workCommand);
      }
      if(isEffortCase&&outcome.operation_id===configureId){
        assert.equal(outcome.outcome,'applied','the explicit max setter is acknowledged before the native change');
        commands.push(refreshCommand);
      } else if(isEffortCase&&outcome.operation_id===refreshId){
        assert.equal(outcome.outcome,'applied');
        assertEffortCheckpoint(JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8')));
        effortObservedBeforeGoals=true;
        commands.push(effortGoalCommands[0]);
      } else if(isEffortCase&&effortGoalCommands.some(goal=>goal.operation_id===outcome.operation_id)){
        assert.equal(effortObservedBeforeGoals,true,'native effort is processed before any productive goal');
        const goal=effortGoalCommands[effortGoalIndex];
        assert.equal(outcome.operation_id,goal?.operation_id,'set/edit/resume run in order, once each');
        assert.equal(outcome.outcome,isEffortMismatch?'rejected':'applied');
        if(isEffortMismatch)assert.equal(outcome.details.diagnostic_code,'CONFIGURE_STANDING_EFFORT_BEFORE_GOAL');
        else {
          assert.equal(outcome.details.completion_condition,'native_goal_admitted');
          assert.equal(outcome.details.native_ack.commandId,commandIdFor(goal.operation_id,goal.created_at_ms));
        }
        const checkpoint=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
        assertEffortCheckpoint(checkpoint);
        if(isEffortMismatch)assert.equal(checkpoint.native_pending?.some(([id])=>id===goal.operation_id),false,
          'an explicit effort mismatch creates no native goal intent');
        effortGoalIndex++;
        if(effortGoalIndex<effortGoalCommands.length)commands.push(effortGoalCommands[effortGoalIndex]);
        else finishedResolve(outcome);
      } else if(isPendingConfigureRecovery&&outcome.operation_id===effortId&&outcome.outcome==='applied'){
        commands.push(configureCommand);
      } else if(isPendingConfigureRecovery&&outcome.operation_id===configureId&&outcome.outcome==='accepted'
          &&moduleHelloCount===1){
        const checkpoint=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
        const pending=checkpoint.native_pending?.find(([id])=>id===configureId)?.[1];
        assert.equal(outcome.details.waiting_for,'model_readback','setModel ACK remains an accepted pending outcome');
        assert.equal(pending?.ack?.status,'accepted','the actual setter ACK is checkpointed before restart');
        assert.equal(pending?.resolved,false,'readback A leaves the configure operation unresolved');
        assert.deepEqual(pending?.setting,{key:'model',desired:{modelId:'fixture-model-B'},method:'session/setModel',
          params:{sessionId,model:{modelId:'fixture-model-B'}}});
        assert.equal(checkpoint.route_defaults.modelId,'fixture-model','the requested B does not replace route A before readback');
        assert.equal(checkpoint.route_defaults.providerId,'fixture-provider');
        assert.equal(checkpoint.standing_effort,'max','the work-starting goal precondition is durably configured');
        configureAcceptedResolve(outcome);
      } else if(isPendingConfigureRecovery&&outcome.operation_id===recoveryId){
        assert.equal(outcome.outcome,'applied','restored native readiness precedes replies, refresh and work attempts');
        commands.push(workCommand,dispatchCommand,goalCommand,replyCommand,refreshCommand);
      } else if(isPendingConfigureRecovery&&fencedWorkIds.has(outcome.operation_id)){
        assert.equal(outcome.outcome,'rejected','productive work stays held behind the restored model configure');
        assert.equal(outcome.details.diagnostic_code,'MODEL_CONFIGURATION_RECONCILIATION_REQUIRED');
        fencedWorkOutcomes.set(outcome.operation_id,outcome);
        queuePendingModelReconcile();
      } else if(isPendingConfigureRecovery&&outcome.operation_id===replyId){
        assert.equal(outcome.outcome,'applied','native replies remain usable while model work is fenced');
        pendingReplyOutcome=outcome;queuePendingModelReconcile();
      } else if(isPendingConfigureRecovery&&outcome.operation_id===refreshId){
        assert.equal(outcome.outcome,'applied','native refresh remains usable while model work is fenced');
        pendingRefreshOutcome=outcome;queuePendingModelReconcile();
      } else if(isPendingConfigureRecovery&&outcome.operation_id===reconcilePendingId){
        assert.equal(outcome.outcome,'applied');
        assert.equal(outcome.details.target_operation_id,configureId);
        assert.equal(outcome.details.disposition,'pending_application','readback A does not resolve requested model B');
        assert.equal(outcome.details.resubmitted,false,'ACK-backed reconciliation does not resend setModel');
        await writeFile(selectedModelPath,JSON.stringify({modelId:'fixture-model-B',providerId:'fixture-native-provider-B'}));
        commands.push(reconcileResolvedCommand);
      } else if(isPendingConfigureRecovery&&outcome.operation_id===reconcileResolvedId){
        assert.equal(outcome.outcome,'applied');
        assert.equal(outcome.details.target_operation_id,configureId);
        assert.equal(outcome.details.disposition,'resolved','the requested model readback resolves the original configure');
        assert.equal(outcome.details.resubmitted,false,'exact readback still does not resend setModel');
        commands.push(postReconcileWorkCommand);
      } else if(isPendingConfigureRecovery&&outcome.operation_id===postReconcileWorkId){
        assert.equal(outcome.outcome,'applied','productive work resumes after the matching readback');
        finishedResolve(outcome);
      } else if(outcome.operation_id===workId){
        workOutcomeCount++;
        assert.equal(workOutcomeCount,1,'the original work command produces one host outcome');
        if(isCheckpointRace){
          assert.equal(outcome.outcome,'rejected','checkpoint race rejects before native admission');
          assert.equal(outcome.details.diagnostic_code,'NATIVE_WORK_SNAPSHOT_CHANGED_BEFORE_REQUEST');
          const checkpoint=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
          checkpointAtWorkOutcome=checkpoint.native_pending?.find(([id])=>id===workId)?.[1];
          assert.ok(checkpointAtWorkOutcome,'the original durable work intent remains readable');
          assert.equal(checkpointAtWorkOutcome.ack,null,'pre-request rejection does not forge a native ACK');
          assert.equal(checkpointAtWorkOutcome.id,commandIdFor(workId,workAt),'same native command ID remains durable');
          assert.equal(checkpointAtWorkOutcome.method,isGoal?'goal/set':'turn/start');
          assert.deepEqual(checkpointAtWorkOutcome.params,isGoal
            ?{sessionId,objective:'fixture goal must stay fenced'}
            :{sessionId,input:[{type:'text',text:sentText}],reasoningEffort:'max',ifBusy:'queue'});
          assert.equal(checkpointAtWorkOutcome.workFenceRejected,true);
          assert.equal(checkpointAtWorkOutcome.workFenceRejectionCode,'NATIVE_WORK_SNAPSHOT_CHANGED_BEFORE_REQUEST');
          assert.deepEqual(checkpointAtWorkOutcome.workFence,{rootId:sessionId,eventVersion:isGoal?1:0,modelId:'fixture-model',
            providerId:'fixture-provider',status:'idle',activeTurnId:null});
          deferredWorkAck={socket,packet};
          setTimeout(()=>commands.push(reconcileCommand),25);
          return;
        }
        reply(socket,packet);
        finishedResolve(outcome);
        return;
      }
      if(outcome.operation_id===reconcileId){
        reconcileOutcomeCount++;
        assert.equal(reconcileOutcomeCount,1,'the guarded explicit reconciliation runs once');
        reply(socket,packet);
        finishedResolve({work:outcomes.get(workId),reconcile:outcome});
        return;
      }
    }
    reply(socket,packet,result);
  }
  try {
    await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(endpoint,resolve);});
    const credentialFile=path.join(dir,'credential.json');
    const configFile=path.join(dir,'module.json');
    await writeFile(credentialFile,JSON.stringify({client_id:'muse-fixture-client',token:'fixture-only'}));
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.13',
      command:process.execPath,args:[nativeScript,'--home',home,'--audit',auditPath,'--fixtures',fixturePath]}));
    const env={PATH:process.env.PATH,ELIOT_SWARM_MODULE_STATE:stateDir,ELIOT_SWARM_MODULE_OWNER:ownerFile,
      TEMP:os.tmpdir(),TMP:os.tmpdir(),SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR};
    if(isCheckpointRace){
      env.NODE_OPTIONS=`--import=${pathToFileURL(gateScript).href}`;
      env.MUSE_FIXTURE_CHECKPOINT_PATH=path.join(stateDir,'checkpoint.json');
      env.MUSE_FIXTURE_CHECKPOINT_OPERATION=workId;
      env.MUSE_FIXTURE_GATE_SIGNAL=gateSignal;
      env.MUSE_FIXTURE_GATE_RELEASE=gateRelease;
    }
    function spawnBridge(){
      bridge=spawn(process.execPath,[path.join(here,'bridge.mjs'),'--config',configFile],
        {cwd:here,env,windowsHide:true,stdio:['ignore','ignore','pipe']});
      bridge.stderr.setEncoding('utf8');bridge.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4000);});
      closed=new Promise(resolve=>bridge.once('close',(code,signal)=>resolve({code,signal})));
    }
    spawnBridge();
    if(isPendingConfigureRecovery){
      const configureFailure=finished.then(
        ()=>{throw new Error('WORK_FENCE_FINISHED_BEFORE_CONFIGURE_ACK');},
        error=>{throw error;});
      const accepted=await Promise.race([configureAccepted,configureFailure,
        delay(15000).then(()=>{throw new Error(`pending configure ACK fixture timed out: ${stderr}`);})]);
      assert.equal(accepted.outcome,'accepted');
      const firstBridge=bridge,firstClosed=closed;
      firstBridge.kill('SIGTERM');
      closeResult=await Promise.race([firstClosed,delay(5000).then(()=>null)]);
      assert.ok(closeResult,'the first actual bridge process checkpoints and stops before restart');
      const restored=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
      const pending=restored.native_pending?.find(([id])=>id===configureId)?.[1];
      assert.equal(pending?.ack?.status,'accepted');
      assert.equal(pending?.resolved,false);
      assert.equal(restored.route_defaults.modelId,'fixture-model');
      assert.equal(restored.route_defaults.providerId,'fixture-provider');
      assert.equal(restored.standing_effort,'max');
      spawnBridge();
    }
    const outcome=await Promise.race([finished,delay(15000).then(()=>{throw new Error(`work-fence fixture timed out: ${stderr}`);})]);
    if(isPendingConfigureRecovery){
      assert.equal(outcomes.get(effortId)?.outcome,'applied','standing effort is configured before the model fence scenario');
      assert.equal(outcomes.get(recoveryId)?.outcome,'applied','the restored native session is explicitly resumed');
      assert.equal(outcomes.get(configureId)?.outcome,'applied','the original configure settles only after its requested readback');
      assert.equal(fencedWorkOutcomes.size,3);
      assert.equal(pendingReplyOutcome?.outcome,'applied');
      assert.equal(pendingRefreshOutcome?.outcome,'applied');
      assert.equal(outcome.operation_id,postReconcileWorkId);
      assert.equal(outcome.turn_id,'turn_fixture_started');
    } else {
      assert.equal(outcomes.get(openId)?.outcome,'applied','the fixture root opens before work preflight');
      assert.ok(outcomes.has(workId),'the work command outcome is recorded');
    }
    if(isPendingConfigureRecovery){
      const finalCheckpoint=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
      assert.equal(finalCheckpoint.route_defaults.modelId,'fixture-model-B');
      assert.equal(Object.hasOwn(finalCheckpoint.route_defaults,'providerId'),false,
        'an omitted provider stays unconstrained after model readback');
      assert.equal(finalCheckpoint.latest.requested_provider,'fixture-provider',
        'the original requested provider remains recorded separately');
      assert.deepEqual(finalCheckpoint.latest.model,{modelId:'fixture-model-B',providerId:'fixture-native-provider-B'},
        'observed provider stays a session fact instead of becoming a requested route provider');
      assert.equal(finalCheckpoint.native_pending?.some(([id])=>id===configureId),false,
        'the original configure entry retires only after host acknowledgement of exact readback');
    } else if(isEffortCase){
      assert.equal(effortObservedBeforeGoals,true);
      assert.equal(effortGoalIndex,3,'all productive goal actions are exercised after the native notification');
      assert.equal(outcomes.get(refreshId)?.outcome,'applied');
      assertEffortCheckpoint(JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8')));
    } else if(scenario==='baseline'){
      assert.equal(outcomes.get(workId)?.outcome,'applied');
      assert.equal(outcome.outcome,'applied','a stable session/list snapshot admits one clean dispatch');
      assert.equal(outcome.turn_id,'turn_fixture_started');
      assert.equal(workOutcomeCount,1);
    } else if(scenario==='read-race'){
      assert.equal(outcomes.get(workId)?.outcome,'rejected');
      assert.equal(outcome.outcome,'rejected','a model event during session/list rejects the work-starting goal');
      assert.equal(outcome.details.diagnostic_code,'NATIVE_WORK_SNAPSHOT_CHANGED_DURING_READ');
    } else {
      assert.equal(outcome.work?.outcome,'rejected');
      assert.equal(outcome.reconcile?.outcome,'applied');
      assert.equal(outcome.reconcile?.details?.target_operation_id,workId);
      assert.equal(outcome.reconcile?.details?.disposition,'work_fence_rejected_before_native_request');
      assert.equal(outcome.reconcile?.details?.resubmitted,false);
      assert.equal(reconcileCommandDelivered,true);
    }
  } catch(error) {
    primaryError=error;
  } finally {
    teardownStarted=true;
    if(bridge&&bridge.exitCode===null)bridge.kill('SIGTERM');
    if(bridge&&closed)closeResult=await Promise.race([closed,delay(5000).then(()=>null)]);
    for(const socket of sockets)socket.destroy();
    if(server.listening)await new Promise(resolve=>server.close(resolve));
    try {
      if(primaryError){
        if(isPendingConfigureRecovery){
          let frames=[];
          try{frames=(await readFile(nativeFramesPath,'utf8')).trim().split('\n').filter(Boolean).map(line=>JSON.parse(line));}
          catch(error){frames=[{audit_read_error:error?.message??String(error)}];}
          primaryError.stack=`${primaryError.stack??String(primaryError)}\nHOST_TRACE=${JSON.stringify(hostTrace)}\nNATIVE_FRAME_TRACE=${JSON.stringify(frames)}`;
        }
      } else {
      let nativeWorkMethods,audit;
      if(isPendingConfigureRecovery){
        const frames=(await readFile(nativeFramesPath,'utf8')).trim().split('\n').filter(Boolean).map(line=>JSON.parse(line));
        const initializations=frames.filter(frame=>frame.method==='initialize');
        const starts=frames.filter(frame=>frame.method==='session/start');
        const resumes=frames.filter(frame=>frame.method==='session/resume');
        const frameEvidence=JSON.stringify(frames);
        assert.equal(initializations.length,2,frameEvidence);
        assert.notEqual(initializations[0].instance_id,initializations[1].instance_id,frameEvidence);
        assert.equal(starts.length,1,`one original session/start across both native processes: ${frameEvidence}`);
        assert.equal(starts[0].instance_id,initializations[0].instance_id,frameEvidence);
        assert.equal(starts[0].params.workspaceRoot,home,frameEvidence);
        assert.equal(starts[0].params.modelId,'fixture-model',frameEvidence);
        assert.equal(starts[0].params.providerId,'fixture-provider',frameEvidence);
        assert.equal(resumes.length,1,`the second native process resumes the recorded session: ${frameEvidence}`);
        assert.equal(resumes[0].instance_id,initializations[1].instance_id,frameEvidence);
        assert.equal(resumes[0].params.sessionId,sessionId,frameEvidence);
        assert.equal(resumes[0].params.excludeItems,true,frameEvidence);
        assert.equal(frames.some(frame=>frame.method==='session/start'
          &&frame.instance_id===resumes[0].instance_id),false,
        `recovery did not fall back to a fresh session/start: ${frameEvidence}`);
        assert.equal(frames.filter(frame=>frame.method==='session/setModel').length,1,
          `ACK-backed reconciliation does not resend the original setter: ${frameEvidence}`);
        assert.equal(frames.filter(frame=>frame.method==='session/setReasoningEffort').length,1,frameEvidence);
        assert.ok(frames.filter(frame=>frame.method==='session/read').length>=4,frameEvidence);
        assert.ok(frames.some(frame=>frame.method==='approval/decide'),
          `replies remain available while productive work is fenced: ${frameEvidence}`);
        nativeWorkMethods=frames.filter(frame=>['turn/start','turn/steer','goal/set','goal/edit','goal/resume'].includes(frame.method))
          .map(frame=>frame.method);
        assert.deepEqual(nativeWorkMethods,['turn/start'],'only the post-reconciliation send reaches native work');
        const admittedStart=frames.find(frame=>frame.method==='turn/start');
        assert.equal(admittedStart.params.input?.[0]?.text,'fixture work after exact configure reconciliation',frameEvidence);
        assert.equal(frames.filter(frame=>frame.method==='goal/set').length,0,
          `work-starting goals remain fenced: ${frameEvidence}`);
      } else {
        audit=JSON.parse(await readFile(auditPath,'utf8'));
        assert.equal(audit.session_start_params?.workspaceRoot,home);
        assert.equal(audit.session_start_params?.modelId,'fixture-model');
        assert.equal(audit.session_start_params?.providerId,'fixture-provider');
        nativeWorkMethods=audit.methods.filter(method=>['turn/start','turn/steer','goal/set','goal/edit','goal/resume'].includes(method));
        assert.equal(audit.session_list_count,isEffortCase?(isEffortMismatch?1:4):1,
          'each admitted work action reads once; effort cases first refresh to observe the native change');
        assert.equal(audit.session_list_params?.workspaceRoot,home);
        assert.equal(audit.session_list_params?.limit,200);
        assert.deepEqual(audit.listed_session,{sessionId,modelId:'fixture-model',providerId:'fixture-provider',
          status:'idle',activeTurnId:null});
        assert.equal(audit.session_read_count,0,'ephemeral fixtures stay on the supported session/list API');
        assert.equal(audit.session_resume_count,0,'work fencing never resumes a native session');
      }
      if(isEffortCase){
        assert.deepEqual(nativeWorkMethods,isEffortMismatch?[]:['goal/set','goal/edit','goal/resume'],
          'native defaults permit all productive goals; explicit effort mismatch admits none');
        assert.equal(audit.effort_refresh_notifications,1,'one native effort change precedes the goal phase');
        assert.equal(audit.effort_changed_notifications,isEffortMismatch?2:1);
        assert.equal(audit.session_set_effort_count,isEffortMismatch?1:0,
          'omitted effort does not require or fabricate a controller setter');
        assert.equal(audit.turn_start_count,0);
        assert.equal(audit.turn_steer_count,0);
        for(const goal of effortGoalCommands){
          const action=goal.input.action;
          assert.equal(audit[`goal_${action}_count`],isEffortMismatch?0:1);
          if(!isEffortMismatch)assert.deepEqual(audit[`goal_${action}_params`],{
            sessionId,...(goal.input.objective===undefined?{}:{objective:goal.input.objective}),
            commandId:commandIdFor(goal.operation_id,goal.created_at_ms)},
          'the original goal payload reaches native once without an invented effort override');
        }
      } else if(scenario==='baseline'){
        assert.deepEqual(nativeWorkMethods,['turn/start'],'the baseline sends exactly one clean native work command');
        assert.equal(audit.turn_start_count,1);
        assert.equal(audit.turn_steer_count,0);
        assert.equal(audit.goal_set_count,0);
        assert.equal(audit.turn_start_params?.commandId,commandIdFor(workId,workAt));
        assert.equal(audit.turn_start_params?.sessionId,sessionId);
        assert.equal(audit.turn_start_params?.reasoningEffort,'max');
        assert.equal(audit.turn_start_params?.ifBusy,'queue');
        assert.deepEqual(audit.turn_start_params?.input,[{type:'text',text:sentText}]);
      } else if(!isPendingConfigureRecovery) {
        assert.deepEqual(nativeWorkMethods,[],'a stale session snapshot causes no native work call');
        assert.equal(audit.turn_start_count,0);
        assert.equal(audit.turn_steer_count,0);
        assert.equal(audit.goal_set_count,0);
        assert.equal(audit.goal_edit_count,0);
        assert.equal(audit.goal_resume_count,0);
      }
      if(scenario==='read-race'||scenario==='goal-checkpoint-race'){
        assert.equal(audit.session_set_effort_count,1,'the work-starting goal reaches its own preflight boundary');
        if(scenario==='read-race')
          assert.equal(audit.model_changed_notifications,1,'the adversarial event lands while session/list is pending');
      }
      if(isCheckpointRace){
        assert.equal(audit.checkpoint_gate_released,true,'the adversarial model event follows the durable pending checkpoint');
        assert.equal(audit.model_changed_notifications,1);
        assert.equal(checkpointAtWorkOutcome?.workFenceRejected,true);
        assert.equal(checkpointAtWorkOutcome?.ack,null,'the rejected fence remains distinct from native ACK truth');
      }
      const retained=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
      if(scenario==='read-race')
        assert.equal(retained.native_pending?.some(([id])=>id===workId),false,'a changed preflight read creates no native intent');
      assert.ok(closeResult,'the actual bridge subprocess stops after the account-free native fixture');
      }
    } catch(error) {
      if(primaryError)primaryError.stack=`${primaryError.stack??String(primaryError)}\nFINAL_AUDIT_ERROR=${error?.stack??String(error)}`;
      else primaryError=error;
    } finally {
      await rm(dir,{recursive:true,force:true});
    }
  }
  if(primaryError)throw primaryError;
  console.log(`PASS work fence ${scenario}: actual bridge preserves exact read/checkpoint and no-replay boundaries`);
}

await runWorkFenceCase('baseline');
await runWorkFenceCase('read-race');
await runWorkFenceCase('checkpoint-race');
await runWorkFenceCase('goal-checkpoint-race');
await runWorkFenceCase('pending-configure-recovery');
await runWorkFenceCase('native-effort-default');
await runWorkFenceCase('explicit-effort-mismatch');

// 4b. Omission selects the installed native default; explicit overrides ride
// verbatim. This checks the actual native payloads, not struct shape: an
// omitted session/start carries no modelId/providerId keys, an omitted
// turn/start carries no reasoningEffort key, and the observed model comes
// only from the session/start readback. Explicit coverage stays in the
// baseline work-fence case (session_start_params modelId/providerId exact,
// turn_start_params reasoningEffort exact).
{
  const { modelMatches, retainModelOverride } = await import('./settings.mjs');
  assert.equal(modelMatches({modelId:'native-a',providerId:'chosen'},{providerId:'chosen'}),true);
  assert.equal(modelMatches({modelId:'native-b',providerId:'other'},{providerId:'chosen'}),false);
  const retained = {modelId:'old',providerId:'old-explicit'};
  retainModelOverride(retained,{modelId:'selected'});
  assert.deepEqual(retained,{modelId:'selected'});
  assert.equal(modelMatches({modelId:'selected',providerId:'native-a'},retained),true);
  assert.equal(modelMatches({modelId:'selected',providerId:'native-b'},retained),true);
  retainModelOverride(retained,{modelId:'selected',providerId:'explicit'});
  assert.equal(modelMatches({modelId:'selected',providerId:'native-b'},retained),false);
  assert.equal(modelMatches({modelId:'native-default',providerId:null},{}),true,
    'omitted model selection constrains nothing before readback');
  assert.equal(modelMatches({modelId:'a',providerId:'p'},{modelId:'a',providerId:'p'}),true,
    'explicit selection still matches exactly');
  assert.equal(modelMatches({modelId:'b',providerId:'p'},{modelId:'a'}),false,
    'explicit model mismatch still fails');
  assert.equal(modelMatches({modelId:'a',providerId:'q'},{modelId:'a',providerId:'p'}),false,
    'explicit provider mismatch still fails');

  const dir=await mkdtemp(path.join(os.tmpdir(),'muse-omission-'));
  const stateDir=path.join(dir,'state');
  const home=path.join(dir,'native-home');
  await mkdir(stateDir);await mkdir(home);
  const ownerFile=path.join(stateDir,'owner.json');
  await writeFile(ownerFile,JSON.stringify({version:1,process:{purpose:'module'},token:'fixture-omission-boot'}));
  const nativeScript=path.join(dir,'fake-native.mjs');
  const fixturePath=path.join(dir,'native-fixture.json');
  const auditPath=path.join(dir,'native-audit.json');
  const sessionId='ses_omission_fixture';
  const openId='70000000-0000-4000-8000-000000000001';
  const workId='70000000-0000-4000-8000-000000000002';
  const openAt=AT_MS+200,workAt=AT_MS+201;
  const sentText='fixture-only-omission-payload';
  function command(operationId,createdAtMs,method,input,routeExtra={}) {
    const inputSha256=createHash('sha256').update(canonicalFixtureJson(input),'utf8').digest('hex');
    return {operation_id:operationId,created_at_ms:createdAtMs,method,binding_id:'fixture-binding',generation:1,
      native_root_id:sessionId,route:{runtime:'muse',module_artifact_id:museDescriptor.artifact.artifact_id,...routeExtra},
      input_sha256:inputSha256,input};
  }
  // Omitted model/provider/effort: only the workspace selects the route.
  const openCommand=command(openId,openAt,'agent.open',{},{native_options:{workspaceRoot:home}});
  const workCommand=command(workId,workAt,'agent.send',{delivery:'next_turn',text:sentText});
  await writeFile(fixturePath,JSON.stringify({sessionId,home}));
  await writeFile(nativeScript,String.raw`
import { readFile, writeFile } from 'node:fs/promises';
const arg=name=>{const i=process.argv.indexOf(name);return i<0?null:process.argv[i+1]};
const home=arg('--home'),auditPath=arg('--audit'),fixturePath=arg('--fixtures');
const fixture=JSON.parse(await readFile(fixturePath,'utf8'));
const session={sessionId:fixture.sessionId,status:'idle',activeTurnId:null,modelId:'native-default-model',
  providerId:null,turnCount:0,workspaceRoot:home};
const audit={methods:[],session_start_params:null,session_start_has_modelId:null,
  session_start_has_providerId:null,turn_start_params:null,turn_start_has_reasoningEffort:null,
  session_list_count:0,turn_start_count:0};
const save=()=>writeFile(auditPath,JSON.stringify(audit));
const respond=(id,result,error,after)=>process.stdout.write(JSON.stringify({jsonrpc:'2.0',id,...(error?{error}:{result})})+'\n',()=>after?.());
async function handle(frame){
  if(typeof frame.method!=='string'||!Object.hasOwn(frame,'id'))return;
  audit.methods.push(frame.method);
  if(frame.method==='initialize'){
    await save();respond(frame.id,{museHome:home,serverInfo:{name:'fixture-muse',version:'1.4.0'},
      schema:{fingerprint:'fixture-schema'},sessionDurability:'ephemeral',grantedCapabilities:[]});return;
  }
  if(frame.method==='usage/read'){
    await save();respond(frame.id,null,{code:-32601,message:'Method not found',data:{kind:'methodNotFound'}});return;
  }
  if(frame.method==='session/start'){
    audit.session_start_params=frame.params;
    audit.session_start_has_modelId=Object.hasOwn(frame.params??{},'modelId');
    audit.session_start_has_providerId=Object.hasOwn(frame.params??{},'providerId');
    await save();respond(frame.id,{session});return;
  }
  if(frame.method==='session/list'){
    audit.session_list_count++;await save();respond(frame.id,{sessions:[session],nextCursor:null});return;
  }
  if(frame.method==='approval/listPending'){await save();respond(frame.id,{approvals:[],userInputs:[]});return;}
  if(frame.method==='turn/start'){
    audit.turn_start_count++;audit.turn_start_params=frame.params;
    audit.turn_start_has_reasoningEffort=Object.hasOwn(frame.params??{},'reasoningEffort');
    await save();respond(frame.id,{commandId:frame.params.commandId,status:'accepted',turnId:'turn_omission_started'});return;
  }
  await save();respond(frame.id,{});
}
process.stdin.setEncoding('utf8');
let buffer='';
process.stdin.on('data',chunk=>{
  buffer+=chunk;let end;
  while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
    try{void handle(JSON.parse(line)).catch(()=>process.exit(2));}catch{process.exit(2);}}
});
process.stdin.on('end',()=>process.exit(0));
`);
  const endpoint=process.platform==='win32'?`\\\\.\\pipe\\muse-omission-${randomUUID()}`:path.join(dir,'control.sock');
  const sockets=new Set(),outcomes=new Map(),commands=[openCommand];
  let workQueued=false,bridge,stderr='',teardownStarted=false,closed,closeResult;
  let resolveFinished,rejectFinished;
  const finished=new Promise((resolve,reject)=>{resolveFinished=resolve;rejectFinished=reject;});
  const server=net.createServer(socket=>{
    sockets.add(socket);socket.setEncoding('utf8');let buffer='';
    socket.on('close',()=>sockets.delete(socket));
    socket.on('error',error=>{if(teardownStarted&&error?.code==='ECONNRESET')return;
      rejectFinished(new Error(`omission fixture socket: ${error?.code??error?.message??error}`));});
    socket.on('data',chunk=>{
      buffer+=chunk;let end;
      while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
        let packet;try{packet=JSON.parse(line);}catch{rejectFinished(new Error('omission fixture received invalid JSON'));return;}
        void respondHost(socket,packet).catch(rejectFinished);
      }
    });
  });
  function reply(socket,packet,result={}) {
    socket.write(JSON.stringify({jsonrpc:'2.0',id:packet.id,result})+'\n');
  }
  async function respondHost(socket,packet) {
    let result={};
    if(packet.method==='client.hello')result={client_id:'muse-fixture-client'};
    else if(packet.method==='module.hello'){
      assert.deepEqual(packet.params.module_contract,museModuleContractClaim);
      result={binding_id:'fixture-binding',generation:1,recovery_required:false,native_root_id:null,native_scope_key:null,
        module_contract_negotiation:museModuleContractNegotiation};
    } else if(packet.method==='module.next') {
      if(commands.length)result={command:commands.shift()};
      else await delay(10);
    } else if(packet.method==='module.outcome') {
      const outcome=packet.params;outcomes.set(outcome.operation_id,outcome);
      if(outcome.operation_id===openId&&outcome.outcome==='applied'&&!workQueued){commands.push(workCommand);workQueued=true;}
      if(outcome.operation_id===workId){reply(socket,packet);resolveFinished(outcome);return;}
      reply(socket,packet,result);return;
    }
    reply(socket,packet,result);
  }
  try {
    await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(endpoint,resolve);});
    const credentialFile=path.join(dir,'credential.json');
    const configFile=path.join(dir,'module.json');
    await writeFile(credentialFile,JSON.stringify({client_id:'muse-fixture-client',token:'fixture-only'}));
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.13',
      command:process.execPath,args:[nativeScript,'--home',home,'--audit',auditPath,'--fixtures',fixturePath]}));
    const env={PATH:process.env.PATH,ELIOT_SWARM_MODULE_STATE:stateDir,ELIOT_SWARM_MODULE_OWNER:ownerFile,
      TEMP:os.tmpdir(),TMP:os.tmpdir(),SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR};
    bridge=spawn(process.execPath,[path.join(here,'bridge.mjs'),'--config',configFile],
      {cwd:here,env,windowsHide:true,stdio:['ignore','ignore','pipe']});
    bridge.stderr.setEncoding('utf8');bridge.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4000);});
    closed=new Promise(resolve=>bridge.once('close',(code,signal)=>resolve({code,signal})));
    const outcome=await Promise.race([finished,delay(15000).then(()=>{throw new Error(`omission fixture timed out: ${stderr}`);})]);
    assert.equal(outcomes.get(openId)?.outcome,'applied','the omitted-native-default root opens');
    assert.equal(outcome.outcome,'applied','omitted per-turn effort still admits native input');
    assert.equal(outcome.turn_id,'turn_omission_started');
  } finally {
    teardownStarted=true;
    if(bridge&&bridge.exitCode===null)bridge.kill('SIGTERM');
    if(bridge&&closed)closeResult=await Promise.race([closed,delay(5000).then(()=>null)]);
    for(const socket of sockets)socket.destroy();
    if(server.listening)await new Promise(resolve=>server.close(resolve));
    try {
      const audit=JSON.parse(await readFile(auditPath,'utf8'));
      assert.equal(audit.session_start_has_modelId,false,'omitted modelId stays omitted from the session/start payload');
      assert.equal(audit.session_start_has_providerId,false,'omitted providerId stays omitted from the session/start payload');
      assert.equal(audit.session_start_params?.workspaceRoot,home,'workspace still rides the payload');
      assert.equal(audit.turn_start_has_reasoningEffort,false,'omitted effort stays omitted from the turn/start payload');
      assert.equal(audit.turn_start_params?.sessionId,sessionId);
      assert.deepEqual(audit.turn_start_params?.input,[{type:'text',text:sentText}]);
      assert.equal(audit.turn_start_count,1,'one native turn carries the omitted-effort payload');
      const retained=JSON.parse(await readFile(path.join(stateDir,'checkpoint.json'),'utf8'));
      assert.equal(Object.hasOwn(retained.route_defaults,'modelId'),false,
        'an observed native default never becomes a controller override');
      assert.equal(Object.hasOwn(retained.route_defaults,'providerId'),false);
      assert.equal(retained.latest?.requested_model,null,'omitted request is recorded as null, not invented');
      assert.equal(retained.latest?.requested_model_status,'omitted_native_default');
      assert.equal(retained.latest?.requested_reasoning_effort,null);
      assert.equal(retained.latest?.reasoning_application,'native_default_at_submission');
      assert.deepEqual(retained.latest?.model,{modelId:'native-default-model',providerId:null},
        'effective model is the readback observation, not a pre-start claim');
    } finally {
      await rm(dir,{recursive:true,force:true});
    }
    assert.ok(closeResult,'the omission bridge subprocess stops after the account-free native fixture');
  }
  console.log('PASS omission: session/turn payloads omit unset fields and read back the native default');
}

// 5. The bridge's closed transport validator mirrors the installed host's
// session/start union. Invalid entries fail before any native frame; admitted
// entries keep only validated fields and invent no defaults for mode.
{
  const admitted = nativeMcpServers({
    'local-stdio': { transport:'stdio', command:'C:/tools/probe.exe', mode:'optional' },
    'remote-http': { transport:'streamableHttp', url:'http://127.0.0.1:9/probe' },
  });
  assert.deepEqual(Object.keys(admitted).sort(), ['local-stdio','remote-http']);
  assert.equal(admitted['local-stdio'].transport, 'stdio');
  assert.ok(!('mode' in admitted['remote-http']), 'omitted mode stays omitted');
  assert.deepEqual(nativeMcpServers({}), {}, 'the SDK permits an empty server map');
  const opaqueServerName = 'MCP service / 服务 '.repeat(8);
  const manyServers = Object.fromEntries(Array.from({length:40}, (_,index) =>
    [index === 0 ? opaqueServerName : `fixture-${index}`, {transport:'stdio',command:'fixture-only'}]));
  assert.deepEqual(nativeMcpServers(manyServers), manyServers,
    'SDK Record keys stay opaque and have no invented server-count or name limit');
  assert.throws(() => nativeMcpServers({ bad:{ transport:'bogusPipe' } }));
  assert.throws(() => nativeMcpServers({ bad:{ transport:'stdio' } }));
  assert.throws(() => nativeMcpServers({ bad:{ transport:'streamableHttp' } }));
  assert.throws(() => nativeMcpServers({ bad:{ transport:'stdio', command:'c', extra:'field' } }));
  assert.throws(() => nativeMcpServers({ bad:'not-an-object' }));

  const protoServer = JSON.parse('{"__proto__":{"transport":"stdio","command":"fixture-only"}}');
  const protoAdmitted = nativeMcpServers(protoServer);
  assert.deepEqual(Object.keys(protoAdmitted), ['__proto__']);
  assert.equal(Object.hasOwn(protoAdmitted, '__proto__'), true);
  assert.deepEqual(JSON.parse(JSON.stringify(protoAdmitted)), protoServer,
    'a valid server name is preserved as an own JSON property');

  const requestedServers = nativeMcpServers({
    'required-dead': { transport:'stdio', command:'fixture-only', mode:'required' },
  });
  const failedState = {
    mcp_servers_requested:Object.keys(requestedServers),
    mcp_servers_config_forwarded:null,
    mcp_servers_admission_ack:{session_id:'stale-fixture'},
  };
  let admissionPossible = false;
  await assert.rejects(startNativeMcpSession({
    async command(method,params) {
      assert.equal(method,'session/start');
      assert.deepEqual(params.config.mcpServers,requestedServers);
      assert.equal(admissionPossible,true,'the admission boundary is marked at the command call');
      throw new Error('fixture admission rejected');
    },
  },{workspaceRoot:'C:/fixture',modelId:'fixture-model'},{maxAttempts:1},requestedServers,failedState,
    ()=>{admissionPossible=true;}),/fixture admission rejected/);
  assert.deepEqual(failedState,{
    mcp_servers_requested:['required-dead'],
    mcp_servers_config_forwarded:['required-dead'],
    mcp_servers_admission_ack:null,
  },'failed session/start is never reported as applied or acknowledged');
  assert.equal(Object.hasOwn(failedState,'mcp_servers_applied'),false);

  const acceptedState = {mcp_servers_admission_ack:null};
  await startNativeMcpSession({
    async command() { return {session:{sessionId:'ses_fixture_ack'}}; },
  },{workspaceRoot:'C:/fixture',modelId:'fixture-model'},{maxAttempts:1},requestedServers,acceptedState,()=>{});
  assert.deepEqual(acceptedState.mcp_servers_admission_ack,{session_id:'ses_fixture_ack'},
    'the admission acknowledgement records only the returned session identity');
  console.log('PASS native-mcp-servers: closed transport union admitted without invented defaults');
}
// 9. Recovery discovery through the actual bridge subprocess and fixture native view paging.
// A resume with excludeItems true carries no inline items, so a child born while
// disconnected is missed by metadata refresh plus known child follows. The bridge pages
// the durable parent view for subagent and reminderChild provenance and follows each
// newly proven child with an exact read. No session list family guess, no replay.
{
  async function runRecoveryDiscoveryScenario(scenario) {
    const dir = await mkdtemp(path.join(os.tmpdir(), "muse-recovery-discovery-"));
    const stateDir = path.join(dir, "state");
    const home = path.join(dir, "native-home");
    await mkdir(stateDir);
    await mkdir(home);
    const canonicalHome = await realpath(home);
    const nativeScope = "muse:" + process.platform + ":" + (process.platform === "win32" ? canonicalHome.toLowerCase() : canonicalHome);
    const ownerFile = path.join(stateDir, "owner.json");
    const token = "fixture-recovery-boot-" + scenario;
    await writeFile(ownerFile, JSON.stringify({version: 1, process: {purpose: "module"}, token: token}));
    const rootId = "ses_recovery_root";
    const staleId = "ses_recovery_stale";
    const missedId = "ses_recovery_missed";
    const reminderId = "ses_recovery_reminder";
    const rootSession = {sessionId: rootId, status: "idle", activeTurnId: null, modelId: "fixture-model", providerId: "fixture-provider", turnCount: 2};
    const staleNative = {sessionId: staleId, status: "running", activeTurnId: "turn_stale_current", modelId: "fixture-model", providerId: "fixture-provider", turnCount: 1};
    const missedNative = {sessionId: missedId, status: "running", activeTurnId: "turn_missed_current", modelId: "fixture-model", providerId: "fixture-provider", turnCount: 1};
    const reminderNative = {sessionId: reminderId, status: "idle", activeTurnId: null, modelId: "fixture-model", providerId: "fixture-provider", turnCount: 0};
    const staleCheckpoint = {sessionId: staleId, parentSessionId: rootId, itemId: "itm_stale_parent", subagentId: "sub_stale", parentTurnId: "turn_parent", status: "running", controlStatus: "running", revision: 1, snapshot: {sessionId: staleId, status: "running", activeTurnId: "turn_stale_old", modelId: "fixture-model", providerId: "fixture-provider", turnCount: 1}};
    const savedChildren = scenario === "provenance-pagination-stale-fresh" ? [[staleId, staleCheckpoint]] : [];
    const saved = {client_id: "muse-fixture-client", module_artifact_id: museDescriptor.artifact.artifact_id, boot_id: token, binding: {binding_id: "fixture-binding", generation: 1}, root_id: rootId, native_scope: nativeScope, route_defaults: {workspaceRoot: home, modelId: "fixture-model", providerId: "fixture-provider"}, latest: {execution: "recovery_required", family_completeness: "partial", gaps: 1, observed_children: [], pending_requests: []}, outcomes: [], native_pending: [], children: savedChildren, turns: []};
    await writeFile(path.join(stateDir, "checkpoint.json"), JSON.stringify({version: 1, ...saved}));
    const missedItem = {kind: "subagent", childSessionId: missedId, itemId: "itm_missed", revision: 2, subagentId: "sub_missed", turnId: "turn_parent", status: "running", controlStatus: "running"};
    const reminderItem = {kind: "reminderChild", childSessionId: reminderId, itemId: "itm_reminder", revision: 1, turnId: "turn_parent", status: "running"};
    const noteItem = {kind: "agentMessage", itemId: "itm_note", revision: 1, text: "not a child"};
    const foreignItem = {kind: "subagent", childSessionId: "ses_foreign_child", itemId: "itm_foreign", revision: 1};
    const nokeyItem = {kind: "subagent", itemId: "itm_nokey", revision: 3};
    function viewEvent(item, cursor, seq, owner) {
      return {method: "item/completed", params: {sessionId: owner, viewCursor: cursor, sourceRange: {first: {id: "e" + seq, sequence: seq}, last: {id: "e" + seq, sequence: seq}, stream: {id: owner, kind: "session"}}, item: item}};
    }
    const foreignEvent = viewEvent(foreignItem, "cur_foreign", 9, "ses_foreign_fixture");
    const pagesFull = [{events: [viewEvent(missedItem, "cur_one", 1, rootId), viewEvent(noteItem, "cur_two", 2, rootId), foreignEvent, viewEvent(nokeyItem, "cur_four", 4, rootId), viewEvent(reminderItem, "cur_five", 5, rootId)], nextCursor: "cur_page_two"}, {events: [], nextCursor: null}];
    let pagedEvents = [];
    if (scenario === "beyond-first-pass") {
      pagedEvents = [];
      for (let i = 0; i < 1000; i++) pagedEvents.push(viewEvent({kind: "agentMessage", itemId: "itm_fill_" + i, revision: 1}, "cur_fill_" + i, 100 + i, rootId));
      pagedEvents.push(viewEvent({kind: "subagent", childSessionId: missedId, itemId: "itm_far", revision: 3, subagentId: "sub_far", turnId: "turn_parent", status: "running", controlStatus: "running"}, "cur_far", 9999, rootId));
    }
    const nativeFixture = {scenario: scenario, home: home, rootId: rootId, staleId: staleId, missedId: missedId, reminderId: reminderId, sessions: {root: rootSession, stale: staleNative, missed: missedNative, reminder: reminderNative}, pagesFull: pagesFull, pagedEvents: pagedEvents};
    const fixturePath = path.join(dir, "native-fixture.json");
    const auditPath = path.join(dir, "native-audit.json");
    await writeFile(fixturePath, JSON.stringify(nativeFixture));
    const nativeScript = path.join(dir, "fake-native.mjs");
    await writeFile(nativeScript, String.raw`
import { readFile, writeFile } from "node:fs/promises";
const arg = function(name) { const i = process.argv.indexOf(name); return i < 0 ? null : process.argv[i + 1]; };
const home = arg("--home");
const auditPath = arg("--audit");
const fixturePath = arg("--fixtures");
const fixture = JSON.parse(await readFile(fixturePath, "utf8"));
const scenario = fixture.scenario;
const audit = {methods: [], resumeExcludeItems: [], viewPageCalls: [], sessionReads: [], subscribes: [], forbidden: []};
const save = function() { return writeFile(auditPath, JSON.stringify(audit)); };
const respond = function(id, result, error) { process.stdout.write(JSON.stringify({jsonrpc: "2.0", id: id, ...(error ? {error: error} : {result: result})}) + "\n"); };
function sessionFor(id) {
  if (id === fixture.rootId) return fixture.sessions.root;
  if (id === fixture.staleId) return fixture.sessions.stale;
  if (id === fixture.missedId) return fixture.sessions.missed;
  if (id === fixture.reminderId) return fixture.sessions.reminder;
  return null;
}
async function handle(frame) {
  if (typeof frame.method !== "string" || !Object.hasOwn(frame, "id")) return;
  audit.methods.push(frame.method);
  if (frame.method === "initialize") {
    await save();
    respond(frame.id, {museHome: home, serverInfo: {name: "fixture-muse", version: "1"}, schema: {fingerprint: "fixture-schema"}, sessionDurability: "durable", grantedCapabilities: []});
    return;
  }
  if (frame.method === "usage/read") {
    await save();
    respond(frame.id, null, {code: -32601, message: "Method not found", data: {kind: "methodNotFound"}});
    return;
  }
  if (frame.method === "session/read") {
    audit.sessionReads.push(frame.params.sessionId);
    await save();
    const session = sessionFor(frame.params.sessionId);
    if (!session) {
      respond(frame.id, null, {code: -32004, message: "Fixture session is missing", data: {kind: "notFound"}});
      return;
    }
    respond(frame.id, {session: session, viewCursor: "cursor_read_" + frame.params.sessionId});
    return;
  }
  if (frame.method === "session/resume") {
    audit.resumeExcludeItems.push(frame.params.excludeItems);
    await save();
    if (frame.params.sessionId !== fixture.rootId || frame.params.excludeItems !== true) {
      respond(frame.id, null, {code: -32602, message: "Fixture resume identity violated", data: {kind: "invalidParams"}});
      return;
    }
    respond(frame.id, {session: fixture.sessions.root, viewCursor: "cursor_resume_fixture"});
    return;
  }
  if (frame.method === "view/page") {
    audit.viewPageCalls.push({sessionId: frame.params.sessionId, limit: frame.params.limit, direction: frame.params.direction, cursor: frame.params.cursor === undefined ? null : frame.params.cursor});
    await save();
    if (scenario === "death-while-paging") {
      process.exit(1);
      return;
    }
    if (scenario === "nonadvancing") {
      respond(frame.id, {events: [], nextCursor: "stuck_cursor"});
      return;
    }
    if (scenario === "beyond-first-pass") {
      const all = fixture.pagedEvents;
      let start = 0;
      const atCursor = frame.params.cursor === undefined ? null : frame.params.cursor;
      if (atCursor !== null) {
        const m = /^page_(\d+)$/.exec(atCursor);
        if (!m) {
          respond(frame.id, null, {code: -32602, message: "Fixture cursor unknown", data: {kind: "invalidParams"}});
          return;
        }
        start = parseInt(m[1], 10) * 100;
      }
      const slice = all.slice(start, start + 100);
      const pagesDone = (start / 100) + 1;
      respond(frame.id, {events: slice, nextCursor: start + 100 < all.length ? "page_" + pagesDone : null});
      return;
    }
    const index = audit.viewPageCalls.length - 1;
    const page = fixture.pagesFull[index];
    if (!page) {
      respond(frame.id, null, {code: -32602, message: "Fixture page missing", data: {kind: "invalidParams"}});
      return;
    }
    respond(frame.id, page);
    return;
  }
  if (frame.method === "view/subscribe") {
    audit.subscribes.push(frame.params.sessionId);
    await save();
    respond(frame.id, {});
    return;
  }
  if (frame.method === "approval/listPending") {
    await save();
    respond(frame.id, {approvals: [], userInputs: []});
    return;
  }
  if (frame.method === "session/list") {
    await save();
    respond(frame.id, {sessions: [fixture.sessions.root, fixture.sessions.stale, fixture.sessions.missed, fixture.sessions.reminder], nextCursor: null});
    return;
  }
  const forbidden = ["turn/start", "turn/steer", "session/start", "session/fork", "approval/decide", "userInput/answer", "userInput/cancel", "userInput/clarify", "goal/set", "goal/edit", "goal/clear", "goal/pause", "goal/resume", "session/setModel", "session/setReasoningEffort", "session/setApprovalMode"];
  if (forbidden.indexOf(frame.method) >= 0) audit.forbidden.push(frame.method);
  await save();
  respond(frame.id, {});
}
process.stdin.setEncoding("utf8");
let buffer = "";
process.stdin.on("data", function(chunk) {
  buffer += chunk;
  let end = -1;
  while ((end = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, end);
    buffer = buffer.slice(end + 1);
    if (!line) continue;
    try { void handle(JSON.parse(line)).catch(function() { process.exit(2); }); } catch (e) { process.exit(2); }
  }
});
process.stdin.on("end", function() { process.exit(0); });
`);
    function storeRecoveryCommand(operationId, createdAtMs) {
      const input = {expected_boot_id: token};
      const inputSha256 = createHash("sha256").update(canonicalFixtureJson(input), "utf8").digest("hex");
      return {operation_id: operationId, created_at_ms: createdAtMs, method: "agent.recover", binding_id: "fixture-binding", generation: 1, native_root_id: rootId, route: {runtime: "muse", module_artifact_id: museDescriptor.artifact.artifact_id}, input_sha256: inputSha256, input: input};
    }
    const recoverId = "80000000-0000-4000-8000-000000000001";
    const commands = [storeRecoveryCommand(recoverId, AT_MS)];
    const outcomes = new Map();
    const observations = [];
    function storeRefreshCommand(operationId, createdAtMs) {
      const input = {};
      const inputSha256 = createHash("sha256").update(canonicalFixtureJson(input), "utf8").digest("hex");
      return {operation_id: operationId, created_at_ms: createdAtMs, method: "agent.refresh", binding_id: "fixture-binding", generation: 1, native_root_id: rootId, route: {runtime: "muse", module_artifact_id: museDescriptor.artifact.artifact_id}, input_sha256: inputSha256, input: input};
    }
    const refreshId = "80000000-0000-4000-8000-000000000002";
    let refreshQueued = false;
    let resolveFinished = null;
    let rejectFinished = null;
    const finished = new Promise(function(resolve, reject) { resolveFinished = resolve; rejectFinished = reject; });
    function maybeAdvance() {
      const state = observations[observations.length - 1];
      const recoveryOutcome = outcomes.get(recoverId);
      if (recoveryOutcome && recoveryOutcome.outcome !== "applied") {
        rejectFinished(new Error("agent.recover did not apply in " + scenario + ": " + JSON.stringify(recoveryOutcome)));
        return;
      }
      const ready = recoveryOutcome && recoveryOutcome.outcome === "applied" && state && state.recovery && state.recovery.status === "resumed";
      if (!ready) return;
      const kids = state.observed_children || [];
      const byId = new Map(kids.map(function(c) { return [c.sessionId, c]; }));
      if (scenario === "provenance-pagination-stale-fresh") {
        const stale = byId.get(staleId);
        const missed = byId.get(missedId);
        const reminder = byId.get(reminderId);
        if (stale && stale.snapshot_freshness === "fresh" && stale.snapshot && stale.snapshot.activeTurnId === "turn_stale_current" && missed && missed.snapshot_freshness === "fresh" && missed.snapshot && missed.snapshot.activeTurnId === "turn_missed_current" && missed.itemId === "itm_missed" && missed.revision === 2 && missed.parentSessionId === rootId && missed.parentTurnId === "turn_parent" && reminder && reminder.snapshot_freshness === "fresh" && reminder.itemId === "itm_reminder" && reminder.parentSessionId === rootId) resolveFinished();
      } else if (scenario === "nonadvancing") {
        if ((state.gaps || 0) >= 3 && kids.length === 0 && !state.recovery_discovery) resolveFinished();
      } else if (scenario === "death-while-paging") {
        if ((state.gaps || 0) >= 3 && !byId.has(missedId) && !state.recovery_discovery) resolveFinished();
      } else if (scenario === "beyond-first-pass") {
        if (!refreshQueued) {
          if (state.recovery_discovery && state.recovery_discovery.truncated === true && (state.recovery_discovery.discovered_count || 0) === 0) {
            refreshQueued = true;
            commands.push(storeRefreshCommand(refreshId, AT_MS + 1));
          }
        } else {
          const far = byId.get(missedId);
          if (far && far.snapshot_freshness === "fresh" && far.itemId === "itm_far" && far.revision === 3 && outcomes.get(refreshId) && outcomes.get(refreshId).outcome === "applied") resolveFinished();
        }
      }
    }
    const pipeLead = String.fromCharCode(92, 92, 46, 92);
    const endpoint = process.platform === "win32" ? pipeLead + "pipe" + String.fromCharCode(92) + "muse-recovery-" + scenario + "-" + randomUUID() : path.join(dir, "control.sock");
    const server = net.createServer();
    const sockets = new Set();
    let teardownStarted = false;
    async function respondHost(socket, packet) {
      let result = {};
      if (packet.method === "client.hello") result = {client_id: "muse-fixture-client"};
      else if (packet.method === "module.hello") {
        assert.deepEqual(packet.params.module_contract, museModuleContractClaim);
        result = {binding_id: "fixture-binding", generation: 1, recovery_required: true, native_root_id: rootId, native_scope_key: nativeScope, module_contract_negotiation: museModuleContractNegotiation};
      } else if (packet.method === "module.next") {
        if (commands.length) result = {command: commands.shift()};
        else await delay(10);
      } else if (packet.method === "module.outcome") {
        outcomes.set(packet.params.operation_id, packet.params);
        maybeAdvance();
      } else if (packet.method === "module.observe") {
        observations.push(packet.params.state);
        maybeAdvance();
      }
      socket.write(JSON.stringify({jsonrpc: "2.0", id: packet.id, result: result}) + "\n");
    }
    server.on("connection", function(socket) {
      sockets.add(socket);
      socket.setEncoding("utf8");
      let buffer = "";
      socket.on("close", function() { sockets.delete(socket); });
      socket.on("error", function(error) {
        if (teardownStarted && error && error.code === "ECONNRESET") return;
        rejectFinished(new Error("recovery discovery host socket in " + scenario + ": " + ((error && error.code) || (error && error.message) || String(error))));
      });
      socket.on("data", function(chunk) {
        buffer += chunk;
        let end = -1;
        while ((end = buffer.indexOf("\n")) >= 0) {
          const line = buffer.slice(0, end);
          buffer = buffer.slice(end + 1);
          if (!line) continue;
          let packet = null;
          try { packet = JSON.parse(line); } catch (e) { rejectFinished(new Error("recovery discovery host received invalid JSON in " + scenario)); return; }
          void respondHost(socket, packet).catch(rejectFinished);
        }
      });
    });
    let bridge = null;
    let stderr = "";
    let closed = null;
    try {
      await new Promise(function(resolve, reject) { server.once("error", reject); server.listen(endpoint, resolve); });
      const credentialFile = path.join(dir, "credential.json");
      const configFile = path.join(dir, "module.json");
      await writeFile(credentialFile, JSON.stringify({client_id: "muse-fixture-client", token: "fixture-only"}));
      await writeFile(configFile, JSON.stringify({endpoint: endpoint, credentialFile: credentialFile, moduleArtifactId: "muse-sdk-1.3.0-bridge.13", command: process.execPath, args: [nativeScript, "--home", home, "--audit", auditPath, "--fixtures", fixturePath]}));
      const env = {PATH: process.env.PATH, ELIOT_SWARM_MODULE_STATE: stateDir, ELIOT_SWARM_MODULE_OWNER: ownerFile, TEMP: os.tmpdir(), TMP: os.tmpdir(), SystemRoot: process.env.SystemRoot, WINDIR: process.env.WINDIR};
      bridge = spawn(process.execPath, [path.join(here, "bridge.mjs"), "--config", configFile], {cwd: here, env: env, windowsHide: true, stdio: ["ignore", "ignore", "pipe"]});
      bridge.stderr.setEncoding("utf8");
      bridge.stderr.on("data", function(chunk) { stderr = (stderr + chunk).slice(-4000); });
      closed = new Promise(function(resolve) { bridge.once("close", function(code, signal) { resolve({code: code, signal: signal}); }); });
      const deadline = Date.now() + 25000;
      let done = false;
      while (Date.now() < deadline) {
        if (bridge.exitCode !== null) throw new Error("recovery discovery bridge exited early in " + scenario + " (" + bridge.exitCode + "): " + stderr);
        const won = await Promise.race([finished.then(function() { return true; }), delay(50).then(function() { return false; })]);
        if (won) { done = true; break; }
      }
      if (!done) throw new Error("recovery discovery timed out in " + scenario + ": " + stderr);
      assert.equal(outcomes.get(recoverId).outcome, "applied", "explicit resume admits the retained root in " + scenario);
    } finally {
      teardownStarted = true;
      if (bridge && bridge.exitCode === null) bridge.kill("SIGTERM");
      if (closed) await Promise.race([closed, delay(5000).then(function() { return null; })]);
      for (const socket of sockets) socket.destroy();
      if (server.listening) await new Promise(function(resolve) { server.close(resolve); });
      if (server.listening) await new Promise(function(resolve) { server.close(resolve); });
      let audit = null;
      try {
        try {
          audit = JSON.parse(await readFile(auditPath, "utf8"));
        } catch (e) {
          throw new Error("recovery discovery missing native audit in " + scenario + ": " + stderr + " outcomes=" + JSON.stringify([...outcomes.values()]) + " readError=" + ((e && e.code) || String(e)));
        }
        assert.ok(audit.resumeExcludeItems.length >= 1 && audit.resumeExcludeItems.every(function(v) { return v === true; }), "resume pages history separately with excludeItems true in " + scenario);
        if (scenario === "provenance-pagination-stale-fresh") {
          assert.equal(audit.viewPageCalls.length, 2, "bounded discovery pages twice");
          assert.deepEqual(audit.viewPageCalls[0], {sessionId: rootId, limit: 100, direction: "forward", cursor: null}, "first page starts without cursor");
          assert.deepEqual(audit.viewPageCalls[1], {sessionId: rootId, limit: 100, direction: "forward", cursor: "cur_page_two"}, "opaque cursor relayed forward");
          for (const id of [staleId, missedId, reminderId]) assert.ok(audit.sessionReads.indexOf(id) >= 0, "exact child read follows provenance for " + id);
          assert.ok(audit.subscribes.indexOf(missedId) >= 0 && audit.subscribes.indexOf(reminderId) >= 0, "newly proven children resubscribe");
          assert.deepEqual(audit.forbidden, [], "no replay or new session during recovery");
        } else if (scenario === "nonadvancing") {
          assert.equal(audit.viewPageCalls.length, 2, "guard observes both pages before refusing");
          assert.equal(audit.viewPageCalls[1].cursor, "stuck_cursor", "opaque cursor relayed before nonadvancing refusal");
          assert.deepEqual(audit.forbidden, [], "no replay or new session during recovery");
        } else if (scenario === "death-while-paging") {
          assert.equal(audit.viewPageCalls.length, 1, "pending page never resolves after native death");
          assert.deepEqual(audit.forbidden, [], "no replay or new session during recovery");
        } else if (scenario === "beyond-first-pass") {
          assert.equal(audit.viewPageCalls.length, 11, "continuation walks past the first bounded pass");
          assert.equal(audit.viewPageCalls[10].cursor, "page_10", "refresh resumes from the persisted opaque continuation");
          assert.ok(audit.sessionReads.indexOf(missedId) >= 0, "exact child read follows far provenance");
          assert.ok(audit.subscribes.indexOf(missedId) >= 0, "far child resubscribes");
          assert.deepEqual(audit.forbidden, [], "no replay or new session during recovery");
        }
      } finally {
        await rm(dir, {recursive: true, force: true});
      }
    }
    console.log("PASS recovery discovery " + scenario + ": actual bridge subprocess pages durable parent provenance bounded");
  }
  await runRecoveryDiscoveryScenario("provenance-pagination-stale-fresh");
  await runRecoveryDiscoveryScenario("nonadvancing");
  await runRecoveryDiscoveryScenario("death-while-paging");
  await runRecoveryDiscoveryScenario("beyond-first-pass");
}
console.log('MUSE SELFTEST OK');
