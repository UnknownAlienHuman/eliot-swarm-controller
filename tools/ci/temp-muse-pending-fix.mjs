#!/usr/bin/env node
import { readFileSync, writeFileSync } from 'node:fs';

function replaceExact(path, oldText, newText) {
  const source = readFileSync(path, 'utf8');
  const count = source.split(oldText).length - 1;
  if (count !== 1) throw new Error(`${path}: expected one replacement, found ${count}`);
  writeFileSync(path, source.replace(oldText, newText), 'utf8');
}

const bridge = 'modules/muse/bridge.mjs';
replaceExact(
  bridge,
  `function sessionChanged(sessionId, reason = 'native_event_after_read') {
  if (typeof sessionId !== 'string' || !sessionId) return;
  sessionVersions.set(sessionId, (sessionVersions.get(sessionId)??0)+1);
  invalidateChildSnapshot(sessionId, reason);
}
function replacePendingInventory(sessionId, inventory, expectedVersion) {`,
  `function sessionChanged(sessionId, reason = 'native_event_after_read') {
  if (typeof sessionId !== 'string' || !sessionId) return;
  sessionVersions.set(sessionId, (sessionVersions.get(sessionId)??0)+1);
  invalidateChildSnapshot(sessionId, reason);
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
function replacePendingInventory(sessionId, inventory, expectedVersion) {`
);
replaceExact(
  bridge,
  "      const key = `${prefix}:${required(params,idField)}`;",
  "      const key = pendingRequestKey(sessionId, prefix, required(params,idField));"
);
replaceExact(
  bridge,
  `  if (n.method === 'approval/requested' || n.method === 'approval/updated' || n.method === 'userInput/requested') {
    const key = p.approvalId ? \`approval:\${p.approvalId}\` : \`input:\${p.userInputId}\`;
    const previous = pendingRequests.get(key)?.view.params;
    // approval/updated changes the stage, not the original tool/request identity.
    const params = n.method === 'approval/updated' && previous && previous.sessionId === p.sessionId
      ? {...previous, ...p} : p;
    if (params !== p && p.subagentOrigin === undefined) delete params.subagentOrigin;
    pendingRequests.set(key, {view:{method:n.method,params}});
  }
  if (n.method === 'approval/resolved' || n.method === 'userInput/settled') {
    for (const [key,entry] of pendingRequests) {
      if (entry.view.params?.approvalId === p.approvalId && p.approvalId || entry.view.params?.userInputId === p.userInputId && p.userInputId) pendingRequests.delete(key);
    }
  }`,
  `  if (n.method === 'approval/requested' || n.method === 'approval/updated' || n.method === 'userInput/requested') {
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
  }`
);
replaceExact(
  bridge,
  "    const key=request.method==='approval/request'?`approval:${required(p,'approvalId')}`:`input:${required(p,'userInputId')}`;",
  "    const kind=request.method==='approval/request'?'approval':'input';\n    const key=pendingRequestKey(sessionId,kind,kind==='approval'?required(p,'approvalId'):required(p,'userInputId'));"
);

replaceExact(
  'modules/muse/module.example.json',
  '"moduleArtifactId": "muse-sdk-1.3.0-bridge.7"',
  '"moduleArtifactId": "muse-sdk-1.3.0-bridge.8"'
);

const handoff='docs/remediation/2026-10-07/04-muse-pending.md';
replaceExact(
  handoff,
  '**Статус: код bridge.8 добавлен в PR #30; поведенческая и native-квалификация ещё не выполнены. Draft.**',
  '**Статус: код bridge.8 дополнен точной session-scoped identity pending requests; syntax gate выполняется в PR #30. Поведенческая и native-квалификация остаются итоговой фазой.**'
);
replaceExact(
  handoff,
  `- [ ] Одинаковое поведение для root/child и повторных событий.
- [ ] Наблюдение не вызывает approval decision, reply или новый model turn.`,
  `- [ ] Одинаковое поведение для root/child и повторных событий.
- [ ] Одинаковые native request ID в разных sessions не перезаписывают и не закрывают друг друга.
- [ ] Orphan \`approval/updated\` сохраняется как bounded gap, а не становится actionable request с выдуманной identity.
- [ ] Наблюдение не вызывает approval decision, reply или новый model turn.`
);
replaceExact(
  handoff,
  `По UPDATE.md новый код имеет ID \`muse-sdk-1.3.0-bridge.8\`; пример маршрута,
текущие README и два ID в существующем selftest согласованы. Публикация правки
\`modules/muse/module.example.json\` заблокирована инструментом; в PR этот файл
пока сохраняет bridge.7. До согласования примера PR нельзя активировать или
считать готовым. Исторический checkpoint fixture не переписан. Старые binding/checkpoint не мигрируются,
действующие сервисы не активировались и не останавливались.`,
  `По UPDATE.md новый код имеет ID \`muse-sdk-1.3.0-bridge.8\`; controller example,
module example, README и два ID в существующем selftest согласованы. Исторический
checkpoint fixture не переписан. Старые binding/checkpoint не мигрируются,
действующие сервисы не активировались и не останавливались.

Дополнительный audit pass установил, что \`pendingRequests\` нельзя ключевать
только native approval/user-input ID: все native read/reply методы также требуют
\`sessionId\`, а root и child inventories живут одновременно. Bridge использует
одну exact tuple \`(sessionId, kind, requestId)\` для request, inventory, update и
terminal removal. Частичный \`approval/updated\` без сохранённого original request
не становится полноценным вопросом; публикуется bounded gap без model/reply effect.`
);

console.log('Muse pending patch applied');
