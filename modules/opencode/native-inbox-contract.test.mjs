import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = JSON.parse(
  readFileSync(path.join(MODULE_DIR, "fixtures", "native-inbox-2.0.7.json"), "utf8"),
);
const INSTALLED_ROOT = process.env.ELIOT_OPENCODE_INSTALLED_ROOT ?? MODULE_DIR;

const installedPackage = (packageName, relativePath) => pathToFileURL(
  path.join(INSTALLED_ROOT, "node_modules", "@opencode", packageName, "dist", relativePath),
).href;
const installedEffect = pathToFileURL(
  path.join(INSTALLED_ROOT, "node_modules", "effect", "dist", "index.js"),
).href;

const [{ Schema }, PromptInput, SessionInbox, SessionEvent, SessionMessage, ProtocolSession] =
  await Promise.all([
    import(installedEffect),
    import(installedPackage("schema", "prompt-input.js")),
    import(installedPackage("schema", "session-inbox.js")),
    import(installedPackage("schema", "session-event.js")),
    import(installedPackage("schema", "session-message.js")),
    import(installedPackage("protocol", "groups/session.js")),
  ]);

function roundTrip(schema, value, label) {
  let decoded;
  try {
    decoded = Schema.decodeUnknownSync(schema)(value);
  } catch (error) {
    assert.fail(`${label} did not decode: ${error?.message ?? String(error)}`);
  }
  assert.deepEqual(Schema.encodeSync(schema)(decoded), value, `${label} did not round-trip`);
  return decoded;
}

test("OpenCode 2.0.7 native inbox fixture matches installed DTOs", () => {
  const sessionGroup = ProtocolSession.makeSessionGroup(undefined, undefined);
  const promptEndpoint = sessionGroup.endpoints["session.prompt"];
  assert.equal(promptEndpoint.path, "/api/session/:sessionID/prompt");
  const requestSchema = promptEndpoint.payload.get("application/json").schemas[0];
  const responseSchema = [...promptEndpoint.success][0];

  roundTrip(PromptInput.Prompt, {
    text: FIXTURE.contract.text,
    files: [],
    agents: [],
    skills: [],
  }, "PromptInput.Prompt");
  roundTrip(requestSchema, FIXTURE.request, "session.prompt request");
  roundTrip(responseSchema, FIXTURE.admission, "session.prompt response");
  roundTrip(SessionInbox.Item, FIXTURE.events.enqueued.data.item, "SessionInbox.Item");
  roundTrip(SessionInbox.User, FIXTURE.admission.data, "SessionInbox.User");
  roundTrip(SessionEvent.Created, FIXTURE.events.rootCreated, "session.created");
  roundTrip(SessionEvent.InboxEnqueued, FIXTURE.events.enqueued, "session.inbox.enqueued");
  roundTrip(SessionEvent.InboxDelivered, FIXTURE.events.delivered, "session.inbox.delivered");
  roundTrip(SessionMessage.User, FIXTURE.userMessage, "Session.Message.User");

  assert.deepEqual(FIXTURE.request.metadata, FIXTURE.admission.data.payload.metadata);
  assert.deepEqual(FIXTURE.admission.data.payload, FIXTURE.events.enqueued.data.item.payload);
  assert.equal(FIXTURE.events.rootCreated.durable.seq, 1);
  assert.equal(FIXTURE.events.enqueued.durable.seq, 2);
  assert.equal(FIXTURE.events.delivered.durable.seq, 3);
  assert.equal(FIXTURE.events.enqueued.data.inboxID, FIXTURE.admission.data.id);
  assert.equal(FIXTURE.events.delivered.data.inboxID, FIXTURE.admission.data.id);
  assert.equal(FIXTURE.userMessage.id, FIXTURE.events.delivered.data.inboxID);
  assert.equal(FIXTURE.userMessage.time.created, FIXTURE.events.delivered.created);
  assert.equal(FIXTURE.userMessage.text, FIXTURE.admission.data.payload.text);
  assert.deepEqual(FIXTURE.userMessage.metadata, FIXTURE.admission.data.payload.metadata);
});
