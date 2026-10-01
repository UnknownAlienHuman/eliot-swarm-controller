// Caller-owned process transport for the optional Rust module launcher. MSP
// framing, IDs, request routing and protocol handling still belong to the SDK.
import { spawn } from 'node:child_process';
import { Connection, checkServedFingerprint } from '@muse-code/sdk';

export function spawnOwned(options) {
  const child = spawn(options.command, options.args, {cwd:options.cwd, env:options.env,
    detached:false, stdio:['pipe','pipe','pipe'], windowsHide:true});
  child.stdout.setEncoding('utf8'); child.stderr.setEncoding('utf8');
  child.stderr.on('data',options.onStderr ?? (()=>{}));
  child.stdin.on('error',()=>{});
  const exited = new Promise((resolve,reject)=>{
    child.once('error',reject); child.once('close',(code,signal)=>resolve({code,signal}));
  });
  exited.catch(()=>{});
  let close;
  const connection = new Connection({
    incoming:child.stdout,
    write:chunk=>new Promise((resolve,reject)=>{
      if(child.stdin.destroyed) {reject(new Error('NATIVE_STDIN_CLOSED'));return;}
      child.stdin.write(chunk,'utf8',e=>e?reject(e):resolve());
    }),
    close:flushed=>close??=(async()=>{
      // Explicit shutdown only: EOF after the SDK's submission barrier. No
      // timeout-triggered kill. The outer owner waits for remaining descendants.
      if(flushed)await flushed.catch(()=>{});
      if(!child.stdin.destroyed)child.stdin.end();
      await exited;
    })(),
  });
  let initialized = false;
  return {
    exited,
    onNotification:handler=>connection.onNotification(handler),
    onProtocolError:handler=>connection.onProtocolError(handler),
    onServerRequest:handler=>connection.onServerRequest(handler),
    close:()=>connection.close(),
    async initialize(params) {
      if(initialized)throw new Error('NATIVE_ALREADY_INITIALIZED');
      initialized=true;
      const initializeResult=await connection.request('initialize',params);
      if(typeof initializeResult?.schema?.fingerprint!=='string')throw new Error('MISSING_SCHEMA_FINGERPRINT');
      const fingerprintWarning=checkServedFingerprint(initializeResult.schema.fingerprint);
      connection.notify('initialized');await connection.flush();
      return {connection,initializeResult,fingerprintWarning,exited};
    },
  };
}
