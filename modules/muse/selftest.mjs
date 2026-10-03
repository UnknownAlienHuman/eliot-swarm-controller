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
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import net from 'node:net';
import { randomUUID } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
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
  const staleFixture=await load('exact-turn-stale.json');
  const checkpointFixture=await load('checkpoint-roundtrip.json');
  const dir=await mkdtemp(path.join(os.tmpdir(),'muse-bridge-selftest-'));
  const stateDir=path.join(dir,'state');
  const home=path.join(dir,'native-home');
  await mkdir(stateDir);await mkdir(home);
  const ownerFile=path.join(stateDir,'owner.json');
  const token='fixture-reboot-boot';
  await writeFile(ownerFile,JSON.stringify({version:1,process:{purpose:'module'},token}));
  const nativeScope=`muse:${process.platform}:${process.platform==='win32'?home.toLowerCase():home}`;
  const saved=structuredClone(checkpointFixture.state);
  saved.module_artifact_id='muse-sdk-1.3.0-bridge.7';
  saved.native_scope=nativeScope;
  saved.route_defaults.workspaceRoot=home;
  saved.route_defaults.modelId=childFixture.root_session.modelId;
  saved.route_defaults.providerId=childFixture.root_session.providerId;
  saved.latest={execution:'recovery_required',family_completeness:'partial',gaps:1,observed_children:[],pending_requests:[]};
  saved.children=[[childFixture.checkpoint_child.sessionId,childFixture.checkpoint_child],
    [childFixture.unverified_child.sessionId,childFixture.unverified_child]];
  await writeFile(path.join(stateDir,'checkpoint.json'),JSON.stringify({version:1,...saved}));

  const nativeFixturePath=path.join(dir,'native-fixtures.json');
  const auditPath=path.join(dir,'native-audit.json');
  await writeFile(nativeFixturePath,JSON.stringify({child:childFixture,unknown:unknownFixture,stale:staleFixture}));
  const nativeScript=path.join(dir,'fake-native.mjs');
  await writeFile(nativeScript,String.raw`
import { readFile, writeFile } from 'node:fs/promises';
const arg=name=>{const i=process.argv.indexOf(name);return i<0?null:process.argv[i+1]};
const home=arg('--home'),auditPath=arg('--audit'),fixturePath=arg('--fixtures');
const fixtures=JSON.parse(await readFile(fixturePath,'utf8'));
const audit={methods:[],child_reads:0,unknown_response:null,turn_steer:null,finished:false};
let auditTail=Promise.resolve(),buffer='',rootReads=0;
const save=()=>{const bytes=JSON.stringify(audit);auditTail=auditTail.then(()=>writeFile(auditPath,bytes));return auditTail};
const send=packet=>process.stdout.write(JSON.stringify(packet)+'\n');
const respond=(id,result,error,after)=>process.stdout.write(JSON.stringify({jsonrpc:'2.0',id,...(error?{error}:{result})})+'\n',after);
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
      audit.finished=true;await save();process.exit(0);
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
  const commandIds={
    recover:'40000000-0000-4000-8000-000000000001',
    refresh:'40000000-0000-4000-8000-000000000002',
    stale:'40000000-0000-4000-8000-000000000003',
    nonsettling:'40000000-0000-4000-8000-000000000004',
  };
  const commands=[{
    operation_id:commandIds.recover,created_at_ms:AT_MS,method:'agent.recover',native_root_id:saved.root_id,
    input:{expected_boot_id:token},
  }];
  const outcomes=new Map(),observations=[],rpcMethods=[],deliveredCommands=[];
  let refreshQueued=false,staleQueued=false,nonsettlingQueued=false,resolveFinished,rejectFinished;
  const finished=new Promise((resolve,reject)=>{resolveFinished=resolve;rejectFinished=reject;});
  function maybeAdvance() {
    const state=observations.at(-1);
    const child=state?.observed_children?.find(c=>c.sessionId===childFixture.checkpoint_child.sessionId);
    if(outcomes.has(commandIds.recover)&&child?.last_refresh_attempt?.status==='failed'&&!refreshQueued){
      refreshQueued=true;commands.push({operation_id:commandIds.refresh,created_at_ms:AT_MS+1,method:'agent.refresh',native_root_id:saved.root_id,input:{session_id:childFixture.checkpoint_child.sessionId}});
    }
    if(outcomes.has(commandIds.refresh)&&child?.snapshot_freshness==='fresh'&&!staleQueued){
      staleQueued=true;commands.push({operation_id:commandIds.stale,created_at_ms:AT_MS+2,method:'agent.send',native_root_id:saved.root_id,input:staleFixture.input});
    }
    if(outcomes.has(commandIds.stale)&&child?.snapshot_freshness==='stale'
        &&child?.snapshot_freshness_reason===childFixture.expected.after_event_snapshot_freshness_reason
        &&!outcomes.has(commandIds.nonsettling)&&!nonsettlingQueued){
      nonsettlingQueued=true;commands.push({operation_id:commandIds.nonsettling,created_at_ms:AT_MS+3,method:'agent.send',native_root_id:saved.root_id,input:staleFixture.nonsettling.input});
    }
    if(outcomes.has(commandIds.nonsettling))resolveFinished();
  }
  async function respondHost(socket,packet){
    rpcMethods.push(packet.method);
    let result={};
    if(packet.method==='client.hello')result={client_id:'muse-fixture-client'};
    else if(packet.method==='module.hello')result={binding_id:'fixture-binding',generation:1,recovery_required:true};
    else if(packet.method==='module.next'){
      if(commands.length){result={command:commands.shift()};deliveredCommands.push(result.command.method);}
      else await delay(10);
    } else if(packet.method==='module.outcome'){
      const outcome=packet.params;outcomes.set(outcome.operation_id,outcome);maybeAdvance();
    } else if(packet.method==='module.observe'){
      observations.push(packet.params.state);maybeAdvance();
    }
    socket.write(JSON.stringify({jsonrpc:'2.0',id:packet.id,result})+'\n');
  }
  server.on('connection',socket=>{
    sockets.add(socket);socket.setEncoding('utf8');let buffer='';
    socket.on('close',()=>sockets.delete(socket));
    socket.on('data',chunk=>{
      buffer+=chunk;let end;
      while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);if(!line)continue;
        let packet;try{packet=JSON.parse(line);}catch{rejectFinished(new Error('fixture host received invalid JSON'));return;}
        void respondHost(socket,packet).catch(rejectFinished);
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
    await writeFile(configFile,JSON.stringify({endpoint,credentialFile,moduleArtifactId:'muse-sdk-1.3.0-bridge.7',
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
    const debug={delivered:deliveredCommands,outcomes:[...outcomes.keys()],
      observed:observations.slice(-4).map(state=>`${state.execution}:${(state.observed_children??[]).map(child=>`${child.sessionId}/${child.snapshot_freshness}/${child.last_refresh_attempt?.status}`).join(',')}`),
      rpc:[...new Set(rpcMethods)]};
    assert.ok(outcomes.has(commandIds.stale),`bridge did not complete stale-turn fixture: ${stderr}; ${JSON.stringify(debug)}`);
    const auditDeadline=Date.now()+3000;
    let audit;
    while(Date.now()<auditDeadline){
      try{audit=JSON.parse(await readFile(auditPath,'utf8'));}catch{}
      if(audit?.finished)break;
      await delay(25);
    }
    assert.ok(audit?.finished,'fake native fixture did not finish cleanly');
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
    console.log('PASS bridge B5 fixtures: restored-child freshness, retained stale evidence, unknown server request, exact-turn stale rejection');
  } finally {
    if(bridge&&bridge.exitCode===null)bridge.kill();
    if(bridge&&bridge.exitCode===null)await Promise.race([new Promise(resolve=>bridge.once('exit',resolve)),delay(3000)]);
    for(const socket of sockets)socket.destroy();
    if(server.listening)await new Promise(resolve=>server.close(resolve));
    await rm(dir,{recursive:true,force:true});
  }
}

console.log('MUSE SELFTEST OK');
