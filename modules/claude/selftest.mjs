#!/usr/bin/env node
// Fixture self-test for the Claude stream mapper plus the pinned SDK import
// surface. No native executable, account or model call is involved: fixtures
// are authored from the pinned SDK's own message types (see fixtures/*.json).
// Run after `npm ci --ignore-scripts`:  node selftest.mjs
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { query } from '@anthropic-ai/claude-agent-sdk';
import { createStreamState, applySdkMessage, snapshot } from './stream.mjs';

assert.equal(typeof query, 'function', 'pinned SDK must export query()');

const here = path.dirname(fileURLToPath(import.meta.url));
async function load(name) {
  const parsed = JSON.parse(await readFile(path.join(here, 'fixtures', name), 'utf8'));
  assert.ok(Array.isArray(parsed.messages), `${name}: messages array required`);
  return parsed.messages;
}
function feed(messages) {
  const state = createStreamState();
  for (const message of messages) applySdkMessage(state, message);
  return state;
}
function messageById(snap, id) {
  return snap.messages.find(m => m.id === id);
}

// 1. Init identity, multi-frame message assembly, replay, partials, success.
{
  const snap = snapshot(feed(await load('init-success.stream.json')));
  assert.equal(snap.adapter.phase, 'ready');
  assert.equal(snap.adapter.executor_version, '2.1.0-fixture');
  assert.equal(snap.adapter.model, 'claude-fixture-model');
  assert.equal(snap.adapter.permission_mode, 'default');
  assert.equal(snap.partial_events_seen, 2, 'stream_event frames are counted');
  assert.equal(snap.messages_total, 2, 'partials never become messages');
  const first = messageById(snap, 'msg_fixture_1');
  assert.ok(first, 'first message present');
  assert.deepEqual(first.blocks.map(b => b.type), ['thinking', 'text', 'tool_use'],
    'all blocks of the shared message.id survive in arrival order');
  assert.equal(first.frames, 3, 'replayed frame uuid applied once');
  const read = first.blocks.find(b => b.type === 'tool_use');
  assert.equal(read.id, 'toolu_read_1');
  assert.equal(read.result_seen, true);
  assert.equal(read.result_is_error, false);
  assert.equal(snap.observed_children.length, 0);
  assert.equal(snap.family_completeness, 'partial');
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.turns.length, 1);
  assert.equal(snap.turns[0].subtype, 'success');
  assert.equal(snap.usage.total_cost_usd, 0.0123);
  assert.equal(snap.usage.model_usage['claude-fixture-model'].inputTokens, 230);
  assert.equal(snap.init_failure, null);
  console.log('PASS init-success: identity, block assembly, replay, partials, terminal success');
}

// 2. Child linkage and the repeated-message.id block trap inside a child.
{
  const snap = snapshot(feed(await load('child-and-repeat-id.stream.json')));
  assert.equal(snap.observed_children.length, 1);
  const child = snap.observed_children[0];
  assert.equal(child.tool_use_id, 'toolu_task_1');
  assert.equal(child.subagent_type, 'Explore');
  assert.equal(child.status, 'completed', 'root tool_result completes the child');
  assert.equal(child.result_seen, true);
  const childMessage = messageById(snap, 'msg_fixture_20');
  assert.ok(childMessage, 'child message present');
  assert.equal(childMessage.parent_tool_use_id, 'toolu_task_1');
  assert.deepEqual(childMessage.blocks.map(b => b.type), ['text', 'tool_use'],
    'two frames, one message.id, frame-local index 0 each: no block lost');
  const bash = childMessage.blocks.find(b => b.type === 'tool_use');
  assert.equal(bash.id, 'toolu_bash_1');
  assert.equal(bash.result_seen, true, 'child-internal tool result joined by tool id');
  const root = messageById(snap, 'msg_fixture_10');
  assert.deepEqual(root.blocks.map(b => b.type), ['text', 'tool_use']);
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.family_completeness, 'partial', 'one observed child is not a complete family');
  console.log('PASS child-and-repeat-id: parent_tool_use_id linkage, no block lost, child completion');
}

// 3. Init failure is a distinct outcome, never an empty successful start.
{
  const snap = snapshot(feed(await load('init-failure.stream.json')));
  assert.equal(snap.adapter.phase, 'init_failed');
  assert.equal(snap.execution, 'init_failed');
  assert.equal(snap.init_failure.subtype, 'error_during_execution');
  assert.equal(snap.init_failure.errors.length, 1);
  assert.equal(snap.adapter.executor_version, null, 'no identity is invented');
  assert.equal(snap.messages_total, 0);
  console.log('PASS init-failure: distinct init failure, no invented identity');
}

// 4. Terminal error subtype, denials, and cumulative (never summed) usage.
{
  const messages = await load('terminal-errors.stream.json');
  const state = createStreamState();
  for (const message of messages.slice(0, 3)) applySdkMessage(state, message);
  let snap = snapshot(state);
  assert.equal(snap.execution, 'turn_completed');
  assert.equal(snap.permission_denials.length, 1);
  assert.equal(snap.permission_denials[0].tool_use_id, 'toolu_denied_1');
  assert.equal(snap.usage.total_cost_usd, 0.1);
  applySdkMessage(state, messages[3]);
  snap = snapshot(state);
  assert.equal(snap.execution, 'turn_failed');
  assert.equal(snap.turns.length, 2);
  assert.equal(snap.turns[1].subtype, 'error_max_turns');
  assert.equal(snap.usage.total_cost_usd, 0.25, 'latest cumulative total replaces, never sums');
  console.log('PASS terminal-errors: error subtype retained, cumulative usage replaced');
}

console.log('Claude bridge self-test: all fixture assertions passed');
