// Generated wire data only: the official SDK remains the owner of every DTO.
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const SCHEMA_FILE = path.resolve(
  MODULE_DIR, "../../crates/swarm-adapter-opencode/registration/native-durable-2.0.7.schema.json",
);

export async function generatedDurableSchema() {
  const installedRoot = process.env.ELIOT_OPENCODE_INSTALLED_ROOT ?? MODULE_DIR;
  const packages = [
    ["@opencode/schema", "2.0.7"],
    ["effect", "4.0.0-rc.112"],
  ];
  for (const [name, version] of packages) {
    const manifest = JSON.parse(readFileSync(path.join(installedRoot, "node_modules", name, "package.json"), "utf8"));
    assert.equal(manifest.version, version, `durable schema requires exact ${name} ${version}`);
  }
  const [{ Schema }, { Durable }, Form] = await Promise.all([
    import(pathToFileURL(path.join(installedRoot, "node_modules/effect/dist/index.js")).href),
    import(pathToFileURL(path.join(installedRoot, "node_modules/@opencode/schema/dist/session-event.js")).href),
    import(pathToFileURL(path.join(installedRoot, "node_modules/@opencode/schema/dist/form.js")).href),
  ]);
  const document = Schema.toJsonSchemaDocument(Schema.Union([Durable, Form.Info, Form.Detail]));
  assert.equal(document.dialect, "draft-2020-12");
  return `${JSON.stringify({
    $schema: "https://json-schema.org/draft/2020-12/schema",
    $comment: "Generated from @opencode/schema 2.0.7 SessionEvent.Durable, Form.Info and Form.Detail with effect 4.0.0-rc.112 Schema.toJsonSchemaDocument. MIT, Copyright (c) 2025 opencode. See OPENCODE-SCHEMA-LICENSE.txt. Regenerate using modules/opencode/native-durable-schema.mjs --write.",
    $ref: "#/$defs/Session.Event.Durable",
    $defs: document.definitions,
  }, null, 2)}\n`;
}

export async function verifyDurableSchema() {
  assert.equal(readFileSync(SCHEMA_FILE, "utf8"), await generatedDurableSchema(),
    "compiled native durable schema differs from the complete pinned official SDK union");
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv[2] === "--write" && process.argv.length === 3) {
    writeFileSync(SCHEMA_FILE, await generatedDurableSchema());
  } else if (process.argv.length === 2 || (process.argv[2] === "--check" && process.argv.length === 3)) {
    await verifyDurableSchema();
  } else {
    throw new Error("use --check or --write");
  }
}
