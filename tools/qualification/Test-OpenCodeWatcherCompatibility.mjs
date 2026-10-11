// Exercise the wrapper with the native binding loaded by the installed core.
// No OpenCode service, provider, or model process is started.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { resolve, join, relative } from 'node:path';
import { pathToFileURL } from 'node:url';

const [moduleRootArg, outputRootArg] = process.argv.slice(2);
assert(moduleRootArg && outputRootArg, 'Usage: bun Test-OpenCodeWatcherCompatibility.mjs <module-root> <output-root>');
assert(!process.env.OPENCODE_PARCEL_WATCHER_PATH, 'An ambient native-binding override would invalidate this check');
const moduleRoot = resolve(moduleRootArg);
const outputRoot = resolve(outputRootArg);
const corePackage = join(moduleRoot, 'node_modules', '@opencode', 'core', 'package.json');
const requireFromCore = createRequire(corePackage);
const core = JSON.parse(await readFile(corePackage, 'utf8'));
assert.equal(core.version, '2.0.7');
const wrapperPath = requireFromCore.resolve('@parcel/watcher/wrapper');
const watcherPackagePath = requireFromCore.resolve('@parcel/watcher/package.json');
const watcher = JSON.parse(await readFile(watcherPackagePath, 'utf8'));
const loaderPath = join(moduleRoot, 'node_modules', '@opencode', 'core', 'dist', 'filesystem', 'watcher-binding.js');
const { default: loadBinding } = await import(pathToFileURL(loaderPath).href);
const nativeName = `@parcel/watcher-${process.platform}-${process.arch}${process.platform === 'linux' ? '-glibc' : ''}`;
const nativePackage = JSON.parse(await readFile(requireFromCore.resolve(`${nativeName}/package.json`), 'utf8'));
const { createWrapper } = requireFromCore('@parcel/watcher/wrapper');
const api = createWrapper(loadBinding());
const backend = { win32: 'windows', linux: 'inotify', darwin: 'fs-events' }[process.platform];
assert(backend, 'Unsupported native watcher platform');
await mkdir(outputRoot, { recursive: true });
const workspace = await mkdtemp(join(outputRoot, 'watcher-'));
for (const dir of ['nested', '.git', 'node_modules', 'literal-ignore', 'ignored-a', 'ignored-b']) {
  await mkdir(join(workspace, dir));
}
const ignore = ['**/.git/**', '**/node_modules/**', '**/*.tmp', 'literal-ignore', '**/ignored-{a,b}/**'];
const events = [];
const errors = [];
let subscription;
const pause = (ms) => new Promise((done) => setTimeout(done, ms));
const pathOf = (event) => relative(workspace, event.path).replaceAll('\\', '/');
async function waitFor(predicate, label) {
  const deadline = Date.now() + 5000;
  while (!predicate() && Date.now() < deadline) { await pause(20); }
  assert.equal(errors.length, 0, `Native callback failed: ${errors.join(', ')}`);
  assert(predicate(), `Missing native event: ${label}`);
}
try {
  subscription = await api.subscribe(workspace, (error, updates) => {
    if (error) { errors.push(String(error)); return; }
    events.push(...updates);
  }, { ignore, backend });
  await writeFile(join(workspace, 'visible.txt'), 'first\n');
  await writeFile(join(workspace, 'nested', 'source.rs'), 'fn first() {}\n');
  for (const path of ['.git/private', 'node_modules/private', 'nested/hidden.tmp', 'literal-ignore/private', 'ignored-a/private', 'ignored-b/private']) {
    await writeFile(join(workspace, path), 'ignored\n');
  }
  await waitFor(() => ['visible.txt', 'nested/source.rs'].every((path) => events.some((event) => pathOf(event) === path && event.type === 'create')), 'visible and recursive creates');
  await pause(600);
  const ignored = events.filter((event) => /(^|\/)(\.git|node_modules|literal-ignore|ignored-[ab])(\/|$)|\.tmp$/.test(pathOf(event)));
  assert.deepEqual(ignored.map(pathOf), [], 'Ignored paths leaked through the actual native backend');
  await writeFile(join(workspace, 'visible.txt'), 'second\n');
  await waitFor(() => events.some((event) => pathOf(event) === 'visible.txt' && event.type === 'update'), 'visible update');
  await subscription.unsubscribe();
  subscription = undefined;
  const count = events.length;
  await writeFile(join(workspace, 'after-unsubscribe.txt'), 'must not emit\n');
  await pause(600);
  assert.equal(events.length, count, 'Native callbacks continued after unsubscribe');
  const receipt = {
    schema: 'eliot.opencode.watcher_compatibility.v1', status: 'PASS',
    platform: process.platform, runtime: process.versions.bun ?? process.version,
    core_version: core.version, wrapper_version: watcher.version,
    native_package: nativeName, native_version: nativePackage.version,
    wrapper_path: wrapperPath, core_loader_path: loaderPath, workspace, backend, ignore,
    cases: ['core_native_loader', 'recursive_create', 'glob_and_literal_ignore', 'visible_update', 'unsubscribe'],
    events: events.map((event) => ({ type: event.type, path: pathOf(event) })),
    model_calls: 0, native_service_calls: 0,
  };
  console.log(JSON.stringify(receipt));
} finally {
  if (subscription) { await subscription.unsubscribe(); }
}
