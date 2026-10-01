// Small local recovery checkpoint, not a second task database. Only the Rust
// module-run owner holds the OS lock that permits writes to this directory.
import { open, readFile, rename, unlink } from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

export async function recoveryState() {
  const dir=process.env.ELIOT_SWARM_MODULE_STATE;
  const ownerFile=process.env.ELIOT_SWARM_MODULE_OWNER;
  if(!dir && !ownerFile)return null;
  if(!dir || !path.isAbsolute(dir) || ownerFile!==path.join(dir,'owner.json'))throw new Error('INVALID_MODULE_OWNER_PATH');
  const owner=JSON.parse(await readFile(ownerFile,'utf8'));
  if(owner.version!==1 || owner.process?.purpose!=='module' || typeof owner.token!=='string')throw new Error('INVALID_MODULE_OWNER_RECORD');
  const file=path.join(dir,'checkpoint.json');
  let saved;
  try { saved=JSON.parse(await readFile(file,'utf8')); }
  catch(e) {if(e.code!=='ENOENT')throw e;}
  if(saved && saved.version!==1)throw new Error('UNKNOWN_CHECKPOINT_VERSION');
  let tail=Promise.resolve();
  return {owner,saved,
    write(state) {
      // Encode at invocation, not later after mutable maps have changed.
      const bytes=JSON.stringify({version:1,...state});
      const pending=tail.then(async()=>{
        const temp=path.join(dir,`.${randomUUID()}.tmp`);
        try {
          const f=await open(temp,'wx',0o600);
          try {await f.writeFile(bytes,'utf8');await f.sync();}finally{await f.close();}
          await rename(temp,file);
          if(process.platform!=='win32') {const d=await open(dir,'r');try{await d.sync();}finally{await d.close();}}
        } finally {await unlink(temp).catch(()=>{});}
      });
      // A failed persistence boundary remains failed. Never execute another
      // native mutation after silently recovering a broken journal queue.
      tail=pending;return pending;
    },
  };
}
