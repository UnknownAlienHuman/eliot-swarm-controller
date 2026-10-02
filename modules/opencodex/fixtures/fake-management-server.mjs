// Fake OpenCodex Management API server for the fixture selftest. Node
// stdlib only; serves the pinned-contract fixture reconstructions on an
// ephemeral loopback port and records every request (method + path) so
// the selftest can prove the adapter issues GET requests only and never
// touches the data plane (/v1/*). Fixtures are reconstructions from the
// pinned upstream contracts (see each fixture's provenance field), not
// captures from a live opencodex 2.73.0 service.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));

async function fixture(name) {
  return JSON.parse(await readFile(path.join(here, name), 'utf8'));
}

export async function createFakeManagementServer({ scenario = 'happy', token = 'fixture-admin-token' } = {}) {
  const files = {
    health: await fixture('health.json'),
    memory: await fixture(scenario === 'no_launch_marker' ? 'memory-no-launch-marker.json' : 'memory.json'),
    providers: await fixture('providers.json'),
    models: await fixture('models.json'),
    usage: await fixture('usage.json'),
    unauthorized: await fixture('error-401.json'),
    unavailable: await fixture('error-503.json'),
    sibling: await fixture('error-409-sibling.json'),
    catalogBusy: await fixture('error-503-catalog-busy.json'),
    notFound: await fixture('error-404.json'),
    protocols: {
      anthropic: await fixture('protocols-anthropic.json'),
      xai: await fixture('protocols-xai.json'),
      openai: await fixture('protocols-openai.json'),
    },
  };
  if (scenario === 'version_mismatch') {
    files.health = { ...files.health, body: { ...files.health.body, version: '2.74.0-fixture' } };
  }
  const requests = [];
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    requests.push({ method: req.method, path: url.pathname, authed: req.headers['x-opencodex-api-key'] === token });
    const send = (entry) => {
      res.writeHead(entry.status, { 'content-type': 'application/json' });
      res.end(JSON.stringify(entry.body));
    };
    if (url.pathname.startsWith('/v1/')) return send(files.notFound); // data plane: recorded, never served
    if (scenario === 'fail_closed') return send(files.unavailable);
    if (scenario === 'unauthorized' || req.headers['x-opencodex-api-key'] !== token) {
      return send(files.unauthorized);
    }
    if (scenario === 'sibling' && url.pathname === '/api/system/health') return send(files.sibling);
    if (url.pathname === '/api/system/health') return send(files.health);
    if (url.pathname === '/api/system/memory') return send(files.memory);
    if (url.pathname === '/api/providers') return send(files.providers);
    if (url.pathname === '/api/protocols') {
      const entry = files.protocols[url.searchParams.get('provider')];
      return send(entry ?? files.notFound);
    }
    if (url.pathname === '/api/models') {
      return send(scenario === 'catalog_busy' ? files.catalogBusy : files.models);
    }
    if (url.pathname === '/api/usage') return send(files.usage);
    return send(files.notFound);
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    server,
    requests,
    endpoint: `http://127.0.0.1:${port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}
