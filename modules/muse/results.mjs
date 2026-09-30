// On-demand, read-only native result inspection. No resume, model call or result consumption.
import { createHash } from 'node:crypto';
const MAX_PAGE_BYTES = 65536;
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
function text(value, key) {
  if (typeof value?.[key] !== 'string' || !value[key].trim()) throw new Error(`RESULT_MISSING_${key}`);
  return value[key];
}
function integer(value, fallback, minimum = 0) {
  const n = value === undefined ? fallback : value;
  if (!Number.isSafeInteger(n) || n < minimum) throw new Error('INVALID_RESULT_RANGE');
  return n;
}
function canonical(v) {
  if (Array.isArray(v)) return v.map(canonical);
  if (v !== null && typeof v === 'object') return Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])]));
  return v;
}
// Keep only one native page in memory. Cursors are opaque, observed values.
// Search exact historical item revision; never fold the whole conversation or
// silently substitute a newer result for a pinned review.
async function readItem(connection, s) {
  let cursor = s.before_cursor;
  const seen = new Set();
  while (true) {
    const page = await connection.request('view/page', {sessionId:s.session_id,direction:'backward',limit:100,
      ...(cursor===undefined?{}:{cursor})});
    if (!Array.isArray(page.events) || !(page.nextCursor === null || typeof page.nextCursor === 'string')) {
      throw new Error('INVALID_NATIVE_RESULT_PAGE');
    }
    for (const event of [...page.events].reverse()) {
      const p = event.params;
      if (!['item/completed','item/updated','item/started'].includes(event.method)
          || p?.sessionId !== s.session_id || p.item?.itemId !== s.item_id
          || p.item.revision !== s.item_revision) continue;
      text(p,'viewCursor');
      return {item:p.item,cursor:p.viewCursor};
    }
    if (page.nextCursor === null) throw new Error('RESULT_REVISION_NOT_AVAILABLE');
    if (page.nextCursor === cursor || seen.has(page.nextCursor)) throw new Error('NATIVE_PAGINATION_NOT_ADVANCING');
    cursor = page.nextCursor; seen.add(cursor);
  }
}
export async function readResult(connection, input, isMember) {
  const selector=input.selector;
  if (!selector || typeof selector !== 'object' || Array.isArray(selector)) throw new Error('RESULT_SELECTOR_REQUIRED');
  const keys = ['kind','session_id','item_id','item_revision','output_ref','expected_digest','before_cursor'];
  if (Object.keys(selector).some(k=>!keys.includes(k))) throw new Error('UNKNOWN_RESULT_SELECTOR_FIELD');
  const s = {...selector};
  text(s,'session_id'); text(s,'item_id');
  s.item_revision=integer(s.item_revision,undefined,1);
  if (!isMember(s.session_id)) throw new Error('RESULT_OUTSIDE_OBSERVED_FAMILY');
  if (!['result','message','output','patch'].includes(s.kind)) throw new Error('UNSUPPORTED_RESULT_KIND');
  if (s.before_cursor!==undefined) text(s,'before_cursor');
  if (s.expected_digest!==undefined) text(s,'expected_digest');
  const offset=integer(input.offset_bytes,0), length=integer(input.length_bytes,MAX_PAGE_BYTES,1);
  if (length>MAX_PAGE_BYTES) throw new Error('RESULT_PAGE_TOO_LARGE');
  const {item,cursor}=await readItem(connection,s);
  text(item,'status');
  if (item.status==='inProgress') throw new Error('RESULT_ITEM_NOT_TERMINAL');
  const source={native_session_id:s.session_id,item_id:s.item_id,item_revision:s.item_revision,
    item_kind:item.kind,item_status:item.status,turn_id:item.turnId??null,
    view_cursor:cursor,read_method:'view/page',kind:s.kind};
  let bytes,total,mediaType,sourceDigest=null;
  if (s.kind==='result' || s.kind==='message') {
    if (s.output_ref!==undefined) throw new Error('OUTPUT_REF_NOT_APPLICABLE');
    let full;
    if (s.kind==='result') {
      if(item.kind!=='subagent' || !item.result || typeof item.result!=='object')throw new Error('SUBAGENT_RESULT_UNAVAILABLE');
      text(item,'childSessionId');source.child_session_id=item.childSessionId;
      // Includes structuredData, artifactRefs and evidenceRefs verbatim. These
      // references are data, not permission to fetch arbitrary URLs or paths.
      full=Buffer.from(JSON.stringify(canonical(item.result)),'utf8');mediaType='application/json';
    } else {
      if(item.kind!=='agentMessage' || typeof item.text!=='string')throw new Error('MESSAGE_RESULT_UNAVAILABLE');
      if(item.truncated)throw new Error('MESSAGE_RESULT_TRUNCATED');
      full=Buffer.from(item.text,'utf8');mediaType='text/plain';
    }
    total=full.length;sourceDigest=`sha256:${hash(full)}`;
    if(offset>total)throw new Error('RESULT_OFFSET_OUT_OF_RANGE');
    bytes=full.subarray(offset,Math.min(total,offset+length));
    source.digest_basis='canonical_result_json_or_message_utf8';
  } else {
    const ref=s.kind==='patch'?item.patchRef:item.outputRef;
    if(!ref || ref.availability!=='available')throw new Error('STORED_OUTPUT_UNAVAILABLE');
    if(text(s,'output_ref')!==ref.id)throw new Error('OUTPUT_REFERENCE_MISMATCH');
    total=integer(ref.byteLen,undefined);if(offset>total)throw new Error('RESULT_OFFSET_OUT_OF_RANGE');
    source.output_ref=ref.id;source.native_digest=ref.digest??null;
    sourceDigest=ref.digest??null;
    source.read_method='item/readOutput';
    // outputRef.id only: never read the native ref's URI or local filesystem path.
    const p=await connection.request('item/readOutput',{sessionId:s.session_id,itemId:s.item_id,
      outputRef:ref.id,offsetBytes:offset,lengthBytes:length});
    if(typeof p.content!=='string')throw new Error('INVALID_NATIVE_OUTPUT_CONTENT');
    if(p.encoding==='utf8')bytes=Buffer.from(p.content,'utf8');
    else if(p.encoding==='base64'){
      bytes=Buffer.from(p.content,'base64');
      if(bytes.toString('base64')!==p.content)throw new Error('INVALID_NATIVE_OUTPUT_BASE64');
    }else throw new Error('UNSUPPORTED_NATIVE_OUTPUT_ENCODING');
    if(p.offsetBytes!==offset || p.byteLen!==bytes.length || bytes.length>length
        || offset+bytes.length>total || p.eof!==(offset+bytes.length===total)) throw new Error('NATIVE_OUTPUT_RANGE_MISMATCH');
    mediaType=text(p,'mediaType');
  }
  if(s.expected_digest!==undefined && s.expected_digest!==sourceDigest)throw new Error('RESULT_SOURCE_DIGEST_CHANGED');
  if(bytes.length===0 && offset<total)throw new Error('NATIVE_OUTPUT_NOT_ADVANCING');
  source.content_digest=sourceDigest;
  source.whole_digest_verified = sourceDigest?.startsWith('sha256:') && offset===0 && bytes.length===total
    ? `sha256:${hash(bytes)}`===sourceDigest : false;
  if(sourceDigest?.startsWith('sha256:') && offset===0 && bytes.length===total && !source.whole_digest_verified) {
    throw new Error('NATIVE_OUTPUT_DIGEST_MISMATCH');
  }
  return {source,offset_bytes:offset,byte_length:bytes.length,total_bytes:total,eof:offset+bytes.length===total,
    media_type:mediaType,content_base64:bytes.toString('base64'),page_sha256:hash(bytes)};
}
