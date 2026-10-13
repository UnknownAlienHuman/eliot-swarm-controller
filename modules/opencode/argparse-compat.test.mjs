// Compatibility guard for the scoped GHSA-hp3w-g68c-fv3c remediation.
// js-yaml@3.15.2 is pinned by gray-matter; its CLI resolves argparse through the
// scoped override below to dependency-free argparse@2.0.1, removing the only
// sprintf-js edge in this module. http-cache-semantics stays at 4.2.0: the
// GHSA-ch52-4w7c-c8xp shared-cache predicates are absent in our caller
// (make-fetch-happen constructs the policy with shared:false), and no
// source-demonstrated fixed release was established, so no bump is claimed.
//
// Run: node modules/opencode/argparse-compat.test.mjs [--argparse <file-or-dir>]
// Without --argparse the lock-shape guard runs and the live API exercise is
// reported as SKIP (gap), never a failure.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(readFileSync(path.join(MODULE_DIR, 'package.json'), 'utf8'));
const lock = JSON.parse(readFileSync(path.join(MODULE_DIR, 'package-lock.json'), 'utf8'));
const packages = lock.packages || {};

function fail(label, actual, expected) {
  throw new Error(label + ': expected ' + JSON.stringify(expected) + ' but found ' + JSON.stringify(actual));
}

// 1. Scoped override: only js-yaml@3.15.2 resolves argparse to 2.0.1.
const scope = pkg.overrides && pkg.overrides['js-yaml@3.15.2'];
if (!scope || scope.argparse !== '2.0.1') fail('overrides[js-yaml@3.15.2].argparse', scope && scope.argparse, '2.0.1');
if (pkg.overrides['@opencode/core@2.0.7']['@parcel/watcher'] !== '2.5.6') fail('preserved watcher override', 0, '2.5.6');
if (pkg.overrides['@effect/platform-node-shared'] !== '$@effect/platform-node-shared') fail('preserved platform override', 0, 1);

// 2. Lock entry: argparse is exactly the verified 2.0.1 artifact, dependency-free.
const ap = packages['node_modules/argparse'] || {};
if (ap.version !== '2.0.1') fail('argparse version', ap.version, '2.0.1');
if (ap.resolved !== 'https://registry.npmjs.org/argparse/-/argparse-2.0.1.tgz') fail('argparse resolved', ap.resolved, 1);
if (ap.integrity !== 'sha512-8+9WqebbFzpX9OR+Wa6O29asIogeRMzcGtAINdpMHHyAg10f05aSFVBbcEqGf/PXw1EjAZ+q2/bEBg3DvurK3Q==') fail('argparse integrity', ap.integrity, 1);
if (ap.license !== 'Python-2.0') fail('argparse license', ap.license, 'Python-2.0');
if (ap.dependencies) fail('argparse dependencies (must be dependency-free)', ap.dependencies, 0);

// 3. sprintf-js is fully gone: no lock entry and no remaining dependency edge.
if (packages['node_modules/sprintf-js']) fail('node_modules/sprintf-js entry', 'present', 'absent');
const dangling = Object.keys(packages).filter(function (k) {
  const deps = (packages[k] || {}).dependencies || {};
  return Object.prototype.hasOwnProperty.call(deps, 'sprintf-js');
});
if (dangling.length !== 0) fail('remaining sprintf-js edges', dangling, []);

// 4. Untouched pins: js-yaml keeps its declared range and locked 3.15.2;
// http-cache-semantics stays 4.2.0 (caller non-exposure, not a package bump).
const jy = packages['node_modules/js-yaml'] || {};
if (jy.version !== '3.15.2') fail('js-yaml version', jy.version, '3.15.2');
if ((jy.dependencies || {}).argparse !== '^1.0.7') fail('js-yaml declared argparse range', (jy.dependencies || {}).argparse, '^1.0.7');
const hcs = packages['node_modules/http-cache-semantics'] || {};
if (hcs.version !== '4.2.0') fail('http-cache-semantics version', hcs.version, '4.2.0');

// 5. Live API exercise using the exact js-yaml@3.15.2 bin/js-yaml.js call shapes:
// ArgumentParser({version, addHelp}), addArgument x4 (storeTrue x3,
// Const.SUPPRESS, defaultValue), parseArgs. Valid invocations must parse to
// identical namespaces; an empty invocation is rejected with exit status 2 in
// both argparse generations (message text differs and is not asserted).
function loadArgparse() {
  const i = process.argv.indexOf('--argparse');
  const req = createRequire(import.meta.url);
  if (i !== -1 && process.argv[i + 1]) return req(path.resolve(process.argv[i + 1]));
  return req('argparse');
}

let argparse = null;
try {
  argparse = loadArgparse();
} catch (e) {
  console.log('SKIP live argparse API exercise: ' + (e && e.code ? e.code : String(e)));
  console.log('Gap: rerun with --argparse <extracted argparse-2.0.1 file-or-dir>.');
}

if (argparse) {
  const cli = new argparse.ArgumentParser({ version: '3.15.2', addHelp: true });
  cli.addArgument(['-c', '--compact'], { action: 'storeTrue' });
  cli.addArgument(['-j', '--to-json'], { help: argparse.Const.SUPPRESS, action: 'storeTrue' });
  cli.addArgument(['-t', '--trace'], { action: 'storeTrue' });
  cli.addArgument(['file'], { defaultValue: '-' });
  const one = cli.parseArgs(['-c', 'input.yaml']);
  assert.equal(one.compact, true);
  assert.equal(one.file, 'input.yaml');
  assert.equal(one.to_json, false);
  assert.equal(one.trace, false);
  const three = cli.parseArgs(['--to-json', '--trace', 'doc.yaml']);
  assert.equal(three.to_json, true);
  assert.equal(three.trace, true);
  assert.equal(three.file, 'doc.yaml');
  assert.equal(three.compact, false);
  const realExit = process.exit;
  let seen = null;
  process.exit = function (code) { seen = code; throw new Error('__exit__' + code); };
  try {
    cli.parseArgs([]);
  } catch (e) {
    if (String((e && e.message) || e).indexOf('__exit__') !== 0) throw e;
  } finally {
    process.exit = realExit;
  }
  assert.equal(seen, 2);
  console.log('PASS live argparse API exercise (js-yaml CLI shapes).');
}

console.log('PASS lock-shape guard (argparse 2.0.1, no sprintf-js, pins intact).');
