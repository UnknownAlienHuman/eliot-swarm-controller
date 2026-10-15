import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import test from "node:test";
import { verifyDurableSchema } from "./native-durable-schema.mjs";

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = JSON.parse(
  readFileSync(path.join(MODULE_DIR, "fixtures", "native-inbox-2.0.7.json"), "utf8"),
);
const CONTROLS = JSON.parse(readFileSync(path.join(MODULE_DIR, "fixtures", "native-controls-2.0.7.json"), "utf8"));
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

const [Form, Permission, ProtocolPermission] = await Promise.all([
  import(installedPackage("schema", "form.js")),
  import(installedPackage("schema", "permission.js")),
  import(installedPackage("protocol", "groups/permission.js")),
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

test("compiled durable schema reproduces the complete official SDK union", verifyDurableSchema);

test("question forms and permission fixtures use the actual 2.0.7 routes and DTOs", () => {
  const group = ProtocolSession.makeSessionGroup(undefined, undefined);
  for (const [name, method, suffix] of [
    ["list", "GET", ""], ["get", "GET", "/:formID"],
    ["reply", "POST", "/:formID/reply"], ["cancel", "DELETE", "/:formID"],
  ]) {
    const endpoint = group.endpoints[`session.form.${name}`];
    assert.equal(endpoint.method, method);
    assert.equal(endpoint.path, `/api/session/:sessionID/form${suffix}`);
  }
  roundTrip(Form.Info, CONTROLS.question, "question form");
  roundTrip(Form.Info, CONTROLS.authForm, "unrelated native auth form");
  roundTrip(Form.Reply, CONTROLS.formReply, "form reply");
  roundTrip(Form.Detail, {...CONTROLS.question, state: {status: "answered", answer: CONTROLS.formReply.answer}}, "answered form detail");
  roundTrip(Form.Detail, {...CONTROLS.question, state: {status: "cancelled"}}, "cancelled form detail");
  roundTrip(Permission.Request, CONTROLS.permission, "permission request");
  roundTrip(Permission.Request, {...CONTROLS.permission, id: "percustom"}, "custom permission ID");
  roundTrip(Permission.Request, {...CONTROLS.permission, id: "per"}, "minimal permission ID");
  const permissions = ProtocolPermission.makePermissionGroup(undefined, undefined);
  const endpoint = permissions.endpoints["session.permission.reply"];
  assert.equal(endpoint.method, "POST");
  assert.equal(endpoint.path, "/api/session/:sessionID/permission/:requestID/reply");
  roundTrip(endpoint.payload.get("application/json").schemas[0], CONTROLS.permissionReply, "permission reply");
});

test("OpenCode 2.0.7 native inbox fixture matches installed DTOs", () => {
  const sessionGroup = ProtocolSession.makeSessionGroup(undefined, undefined);
  const promptEndpoint = sessionGroup.endpoints["session.prompt"];
  assert.equal(promptEndpoint.path, "/api/session/:sessionID/prompt");
  assert.equal(sessionGroup.endpoints["session.log"].path, "/api/experimental/session/:sessionID/log");
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
  roundTrip(SessionMessage.Assistant, FIXTURE.assistantMessage, "Session.Message.Assistant");
  assert.equal(sessionGroup.endpoints["session.message"].path, "/api/session/:sessionID/message/:messageID");
  assert.equal("parentID" in FIXTURE.assistantMessage, false);

  assert.deepEqual(FIXTURE.request.metadata, FIXTURE.admission.data.payload.metadata);
  assert.deepEqual(FIXTURE.admission.data.payload, FIXTURE.events.enqueued.data.item.payload);
  assert.equal(FIXTURE.events.rootCreated.durable.seq, 0);
  assert.equal(FIXTURE.events.enqueued.durable.seq, 2);
  assert.equal(FIXTURE.events.delivered.durable.seq, 3);
  assert.equal(FIXTURE.events.enqueued.data.inboxID, FIXTURE.admission.data.id);
  assert.equal(FIXTURE.events.delivered.data.inboxID, FIXTURE.admission.data.id);
  assert.equal(FIXTURE.userMessage.id, FIXTURE.events.delivered.data.inboxID);
  assert.equal(FIXTURE.userMessage.time.created, FIXTURE.events.delivered.created);
  assert.equal(FIXTURE.userMessage.text, FIXTURE.admission.data.payload.text);
  assert.deepEqual(FIXTURE.userMessage.metadata, FIXTURE.admission.data.payload.metadata);
});
