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
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.11',
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
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.11',
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
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.11',
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
  const ownerFile=path.join(stateDir,'owner.json');
  await writeFile(ownerFile,JSON.stringify({version:1,process:{purpose:'module'},token:`fixture-work-fence-${scenario}`}));
  const nativeScript=path.join(dir,'fake-native.mjs');
  const gateScript=path.join(dir,'checkpoint-gate.mjs');
  const fixturePath=path.join(dir,'native-fixture.json');
  const auditPath=path.join(dir,'native-audit.json');
  const gateSignal=path.join(dir,'checkpoint-durable.signal');
  const gateRelease=path.join(dir,'checkpoint-gate.release');
  const sessionId=`ses_work_fence_${scenario}`;
  const identities={baseline:['000000000001','000000000002','000000000003'],
    'read-race':['000000000011','000000000013','000000000012'],
    'checkpoint-race':['000000000021','000000000022','000000000023'],
    'goal-checkpoint-race':['000000000031','000000000033','000000000032']}[scenario];
  if(!identities)throw new Error('UNKNOWN_WORK_FENCE_FIXTURE');
  const isGoal=scenario==='read-race'||scenario==='goal-checkpoint-race';
  const isCheckpointRace=scenario==='checkpoint-race'||scenario==='goal-checkpoint-race';
  const openId=`60000000-0000-4000-8000-${identities[0]}`;
  const workId=`60000000-0000-4000-8000-${identities[1]}`;
  const configureId=`60000000-0000-4000-8000-${identities[2]}`;
  const reconcileId='60000000-0000-4000-8000-000000000023';
  const openAt=AT_MS+100,configureAt=AT_MS+101,workAt=AT_MS+102,reconcileAt=AT_MS+103;
  const sentText=`fixture-only-${scenario}-payload`;
  function command(operationId,createdAtMs,method,input,routeExtra={}) {
    const inputSha256=createHash('sha256').update(canonicalFixtureJson(input),'utf8').digest('hex');
    return {operation_id:operationId,created_at_ms:createdAtMs,method,binding_id:'fixture-binding',generation:1,
      native_root_id:sessionId,route:{runtime:'muse',module_artifact_id:museDescriptor.artifact.artifact_id,...routeExtra},
      input_sha256:inputSha256,input};
  }
  const openCommand=command(openId,openAt,'agent.open',{},
    {native_options:{workspaceRoot:home,modelId:'fixture-model',providerId:'fixture-provider',reasoningEffort:'max'}});
  const configureCommand=command(configureId,configureAt,'agent.configure',{settings:{reasoningEffort:'max'}});
  const workCommand=isGoal
    ?command(workId,workAt,'agent.goal',{action:'set',objective:'fixture goal must stay fenced'})
    :command(workId,workAt,'agent.send',{delivery:'next_turn',text:sentText});
  const reconcileCommand=command(reconcileId,reconcileAt,'agent.reconcile',{operation_id:workId});
  const fixture={scenario,sessionId,home,auditPath,gateSignal,gateRelease,workId,
    workMethod:workCommand.method,workText:sentText};
  await writeFile(fixturePath,JSON.stringify(fixture));
  await writeFile(nativeScript,String.raw`
import { access, readFile, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
const arg=name=>{const i=process.argv.indexOf(name);return i<0?null:process.argv[i+1]};
const home=arg('--home'),auditPath=arg('--audit'),fixturePath=arg('--fixtures');
const fixture=JSON.parse(await readFile(fixturePath,'utf8'));
const session={sessionId:fixture.sessionId,status:'idle',activeTurnId:null,modelId:'fixture-model',
  providerId:'fixture-provider',turnCount:0,workspaceRoot:home};
const audit={methods:[],session_start_params:null,session_list_count:0,session_list_params:null,
  listed_session:null,session_read_count:0,session_resume_count:0,session_set_effort_count:0,
  model_changed_notifications:0,checkpoint_gate_released:false,turn_start_count:0,turn_steer_count:0,
  goal_set_count:0,goal_edit_count:0,goal_resume_count:0,turn_start_params:null,goal_set_params:null};
const save=()=>writeFile(auditPath,JSON.stringify(audit));
const send=frame=>process.stdout.write(JSON.stringify(frame)+'\n');
const respond=(id,result,error,after)=>process.stdout.write(JSON.stringify({jsonrpc:'2.0',id,...(error?{error}:{result})})+'\n',()=>after?.());
const sourceRange=(id,sequence)=>({first:{id,sequence},last:{id,sequence},stream:{id:fixture.sessionId,kind:'session'}});
function modelChanged(){
  audit.model_changed_notifications++;
  return {jsonrpc:'2.0',method:'session/modelChanged',params:{sessionId:fixture.sessionId,
    modelId:'changed-model',providerId:'changed-provider',source:'user',sourceRange:sourceRange('fixture_model_change',2),
    viewCursor:'cursor_fixture_model_change'}};
}
function effortChanged(){
  return {jsonrpc:'2.0',method:'session/reasoningEffortChanged',params:{sessionId:fixture.sessionId,
    reasoningEffort:'max',source:'user',sourceRange:sourceRange('fixture_effort_change',1),
    viewCursor:'cursor_fixture_effort_change'}};
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
  if(!Object.hasOwn(frame,'id'))return;
  audit.methods.push(frame.method);
  if(frame.method==='initialize'){
    await save();respond(frame.id,{museHome:home,serverInfo:{name:'fixture-muse',version:'1.4.0'},
      schema:{fingerprint:'fixture-schema'},sessionDurability:'ephemeral',grantedCapabilities:[]});return;
  }
  if(frame.method==='usage/read'){
    await save();respond(frame.id,null,{code:-32601,message:'Method not found',data:{kind:'methodNotFound'}});return;
  }
  if(frame.method==='session/start'){
    audit.session_start_params=frame.params;await save();respond(frame.id,{session});return;
  }
  if(frame.method==='session/list'){
    audit.session_list_count++;audit.session_list_params=frame.params;
    audit.listed_session={sessionId:session.sessionId,modelId:session.modelId,providerId:session.providerId,
      status:session.status,activeTurnId:session.activeTurnId};
    await save();
    if(fixture.scenario==='read-race'){send(modelChanged());await save();}
    const after=fixture.scenario.endsWith('checkpoint-race')?()=>{void checkpointRace().catch(()=>process.exit(2));}:undefined;
    respond(frame.id,{sessions:[session],nextCursor:null},undefined,after);return;
  }
  if(frame.method==='session/read')audit.session_read_count++;
  if(frame.method==='session/resume')audit.session_resume_count++;
  if(frame.method==='session/setReasoningEffort'){
    audit.session_set_effort_count++;await save();send(effortChanged());
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
    audit.goal_edit_count++;await save();respond(frame.id,{commandId:frame.params.commandId,status:'accepted'});return;
  }
  if(frame.method==='goal/resume'){
    audit.goal_resume_count++;await save();respond(frame.id,{commandId:frame.params.commandId,status:'accepted'});return;
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
  let finishedResolve,finishedReject,bridge,stderr='',teardownStarted=false,closed,closeResult;
  let checkpointAtWorkOutcome;
  const finished=new Promise((resolve,reject)=>{finishedResolve=resolve;finishedReject=reject;});
  const server=net.createServer(socket=>{
    sockets.add(socket);socket.setEncoding('utf8');let buffer='';
    socket.on('close',()=>sockets.delete(socket));
    socket.on('error',error=>{if(teardownStarted&&error?.code==='ECONNRESET')return;
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
    if(packet.method==='client.hello')result={client_id:'muse-fixture-client'};
    else if(packet.method==='module.hello'){
      assert.deepEqual(packet.params.module_contract,museModuleContractClaim);
      result={binding_id:'fixture-binding',generation:1,recovery_required:false,native_root_id:null,native_scope_key:null,
        module_contract_negotiation:museModuleContractNegotiation};
    } else if(packet.method==='module.next') {
      if(!commands.length)await delay(10);
      if(commands.length){
        const command=commands.shift();result={command};
        if(command.method==='agent.reconcile'){
          reconcileCommandDelivered=true;
          if(deferredWorkAck)setTimeout(()=>reply(deferredWorkAck.socket,deferredWorkAck.packet),40);
        }
      }
    } else if(packet.method==='module.outcome') {
      const outcome=packet.params;outcomes.set(outcome.operation_id,outcome);
      if(outcome.operation_id===openId&&outcome.outcome==='applied'){
        if(scenario==='read-race'||scenario==='goal-checkpoint-race')commands.push(configureCommand,workCommand);
        else commands.push(workCommand);
      }
      if(outcome.operation_id===workId){
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
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.11',
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
    bridge=spawn(process.execPath,[path.join(here,'bridge.mjs'),'--config',configFile],
      {cwd:here,env,windowsHide:true,stdio:['ignore','ignore','pipe']});
    bridge.stderr.setEncoding('utf8');bridge.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4000);});
    closed=new Promise(resolve=>bridge.once('close',(code,signal)=>resolve({code,signal})));
    const outcome=await Promise.race([finished,delay(15000).then(()=>{throw new Error(`work-fence fixture timed out: ${stderr}`);})]);
    assert.equal(outcomes.get(openId)?.outcome,'applied','the fixture root opens before work preflight');
    assert.ok(outcomes.has(workId),'the work command outcome is recorded');
    if(scenario==='baseline'){
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
  } finally {
    teardownStarted=true;
    if(bridge&&bridge.exitCode===null)bridge.kill('SIGTERM');
    if(bridge&&closed)closeResult=await Promise.race([closed,delay(5000).then(()=>null)]);
    for(const socket of sockets)socket.destroy();
    if(server.listening)await new Promise(resolve=>server.close(resolve));
    try {
      const audit=JSON.parse(await readFile(auditPath,'utf8'));
      assert.equal(audit.session_start_params?.workspaceRoot,home);
      assert.equal(audit.session_start_params?.modelId,'fixture-model');
      assert.equal(audit.session_start_params?.providerId,'fixture-provider');
      assert.equal(audit.session_list_count,1,'one exact session/list row is used for work preflight');
      assert.equal(audit.session_list_params?.workspaceRoot,home);
      assert.equal(audit.session_list_params?.limit,200);
      assert.deepEqual(audit.listed_session,{sessionId,modelId:'fixture-model',providerId:'fixture-provider',
        status:'idle',activeTurnId:null});
      assert.equal(audit.session_read_count,0,'ephemeral fixtures stay on the supported session/list API');
      assert.equal(audit.session_resume_count,0,'work fencing never resumes a native session');
      const nativeWorkMethods=audit.methods.filter(method=>['turn/start','turn/steer','goal/set','goal/edit','goal/resume'].includes(method));
      if(scenario==='baseline'){
        assert.deepEqual(nativeWorkMethods,['turn/start'],'the baseline sends exactly one clean native work command');
        assert.equal(audit.turn_start_count,1);
        assert.equal(audit.turn_steer_count,0);
        assert.equal(audit.goal_set_count,0);
        assert.equal(audit.turn_start_params?.commandId,commandIdFor(workId,workAt));
        assert.equal(audit.turn_start_params?.sessionId,sessionId);
        assert.equal(audit.turn_start_params?.reasoningEffort,'max');
        assert.equal(audit.turn_start_params?.ifBusy,'queue');
        assert.deepEqual(audit.turn_start_params?.input,[{type:'text',text:sentText}]);
      } else {
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
    } finally {
      await rm(dir,{recursive:true,force:true});
    }
    assert.ok(closeResult,'the actual bridge subprocess stops after the account-free native fixture');
  }
  console.log(`PASS work fence ${scenario}: actual bridge preserves exact read/checkpoint and no-replay boundaries`);
}

await runWorkFenceCase('baseline');
await runWorkFenceCase('read-race');
await runWorkFenceCase('checkpoint-race');
await runWorkFenceCase('goal-checkpoint-race');

console.log('MUSE SELFTEST OK');
