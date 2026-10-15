import assert from 'node:assert/strict';
import {
  createInteractionManager,
  interactionDigest,
} from './interactions.mjs';

const bindingId = 'fixture-binding';
const generation = 1;
const bridgeBootId = 'fixture-boot';
const nativeRootId = 'fixture-root';
const nativeScopeKey = 'claude:fixture';
const nativeIdentity = { rootId: nativeRootId, scopeKey: nativeScopeKey };
const requests = [];
const cancellations = [];
const manager = createInteractionManager({
  bindingId,
  generation,
  bridgeBootId,
  getNativeIdentity: () => nativeIdentity,
  onRequest: request => requests.push(request),
  onCancel: event => cancellations.push(event),
  secretValues: ['fixture-module-credential'],
});

const questionInput = {
  questions: [{
    question: 'Choose a token: sk-ant-abcdefghijklmnop123456',
    header: 'Choice',
    options: [
      { label: 'Keep', description: 'Continue with the current settings.' },
      { label: 'Stop', description: 'Do not continue.' },
    ],
    multiSelect: false,
  }],
};
const questionToolUseId = 'toolu_question_fixture';
const questionGuard = manager.preToolUse({
  hook_event_name: 'PreToolUse',
  tool_name: 'AskUserQuestion',
  tool_input: questionInput,
  tool_use_id: questionToolUseId,
});
assert.equal(questionGuard.hookSpecificOutput.permissionDecision, 'ask');

const questionAbort = new AbortController();
const questionPromise = manager.canUseTool('AskUserQuestion', questionInput, {
  signal: questionAbort.signal,
  requestId: 'sdk-request-question-fixture',
  toolUseID: questionToolUseId,
  title: 'Choose an option',
});
const publicQuestion = requests.at(-1);
assert.equal(publicQuestion.request_id, 'sdk-request-question-fixture');
assert.equal(publicQuestion.tool_use_id, questionToolUseId);
assert.equal(publicQuestion.kind, 'question');
assert.equal(publicQuestion.input_redacted, true);
assert.equal(JSON.stringify(publicQuestion).includes('sk-ant-abcdefghijklmnop123456'), false);
assert.equal(publicQuestion.input.questions[0].question.includes('[redacted token]'), true);

const displayedQuestion = publicQuestion.input.questions[0].question;
const questionReply = {
  type: 'question',
  updated_input: {
    questions: publicQuestion.input.questions,
    answers: { [displayedQuestion]: 'Keep' },
  },
};
const questionCommand = {
  operation_id: 'fixture-question-reply-operation',
  binding_id: bindingId,
  generation,
  native_root_id: nativeRootId,
  input: {
    request_id: publicQuestion.request_id,
    request_sha256: publicQuestion.request_sha256,
    interaction_kind: 'question',
    reply: questionReply,
    reply_sha256: interactionDigest(questionReply),
  },
};
const questionAck = manager.reply(questionCommand);
assert.equal(questionAck.request_id, publicQuestion.request_id);
assert.equal(questionAck.native_root_id, nativeRootId);
assert.equal(questionAck.reply_sha256, interactionDigest(questionReply));
const questionResult = await questionPromise;
assert.equal(questionResult.behavior, 'allow');
assert.equal(questionResult.toolUseID, questionToolUseId);
assert.deepEqual(questionResult.updatedInput.questions, questionInput.questions);
assert.deepEqual(questionResult.updatedInput.answers, {
  [questionInput.questions[0].question]: 'Keep',
});
assert.deepEqual(manager.snapshot(), []);
assert.throws(() => manager.reply(questionCommand), /INTERACTION_REPLY_MISMATCH/);

const dynamicPrivateKey = 'ghp_0123456789abcdef0123456789abcdef';
const permissionInput = {
  file_path: 'C:\\fixture\\report.txt',
  [dynamicPrivateKey]: 'opaque dynamic field',
};
const permissionToolUseId = 'toolu_permission_fixture';
assert.equal(manager.preToolUse({
  hook_event_name: 'PreToolUse',
  tool_name: 'Read',
  tool_input: permissionInput,
  tool_use_id: permissionToolUseId,
}).hookSpecificOutput.permissionDecision, 'ask');
const permissionAbort = new AbortController();
const permissionPromise = manager.canUseTool('Read', permissionInput, {
  signal: permissionAbort.signal,
  requestId: 'sdk-request-permission-fixture',
  toolUseID: permissionToolUseId,
});
const publicPermission = requests.at(-1);
assert.equal(publicPermission.kind, 'permission');
assert.equal(JSON.stringify(publicPermission).includes('report.txt'), false);
assert.equal(JSON.stringify(publicPermission).includes(dynamicPrivateKey), false);
assert.deepEqual(publicPermission.input.fields.map(field => field.name), ['field_1', 'field_2']);

const changedReply = {
  type: 'permission',
  decision: 'allow',
  updated_input: { file_path: 'C:\\fixture\\changed.txt' },
};
assert.throws(() => manager.reply({
  ...questionCommand,
  operation_id: 'fixture-changed-reply-operation',
  input: {
    request_id: publicPermission.request_id,
    request_sha256: '0'.repeat(64),
    interaction_kind: 'permission',
    reply: changedReply,
  },
}), /INTERACTION_REPLY_MISMATCH/);
assert.equal(manager.snapshot().length, 1, 'a mismatched reply leaves the exact callback pending');

permissionAbort.abort();
const cancelledResult = await permissionPromise;
assert.equal(cancelledResult.behavior, 'deny');
assert.equal(cancelledResult.toolUseID, permissionToolUseId);
assert.equal(cancellations.at(-1).request_id, publicPermission.request_id);
assert.equal(manager.events().at(-1).kind, 'interaction_cancelled');
assert.equal(manager.events().at(-1).request_id, publicPermission.request_id);
assert.equal(manager.snapshot().length, 0);
assert.throws(() => manager.reply({
  ...questionCommand,
  input: {
    request_id: publicPermission.request_id,
    request_sha256: publicPermission.request_sha256,
    interaction_kind: 'permission',
    reply: changedReply,
  },
}), /INTERACTION_REPLY_MISMATCH/);

const retiredToolUseId = 'toolu_retired_fixture';
manager.preToolUse({
  hook_event_name: 'PreToolUse',
  tool_name: 'Read',
  tool_input: { file_path: 'C:\\fixture\\retired.txt' },
  tool_use_id: retiredToolUseId,
});
const retiredAbort = new AbortController();
const retiredPromise = manager.canUseTool('Read', { file_path: 'C:\\fixture\\retired.txt' }, {
  signal: retiredAbort.signal,
  requestId: 'sdk-request-retired-fixture',
  toolUseID: retiredToolUseId,
});
manager.retire('BRIDGE_RETIRED');
assert.equal((await retiredPromise).behavior, 'deny');
assert.equal(manager.snapshot().length, 0);
assert.throws(() => manager.reply({
  ...questionCommand,
  input: {
    request_id: 'sdk-request-retired-fixture',
    request_sha256: requests.at(-1).request_sha256,
    interaction_kind: 'permission',
    reply: changedReply,
  },
}), /INTERACTION_REPLY_IDENTITY/);

console.log('PASS connected-interaction: PreToolUse guard, exact callback IDs, redacted projection, typed answer restoration, abort, retirement and late-reply fence');
