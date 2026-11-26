/**
 * Unified Subagent Mission Control Sidecar Backend (`subagent_mission_control`).
 *
 * Takes over BOTH:
 *   1. OpenAI Codex subagents (backed by `codex app-server` daemon via `codex_bridge_daemon.py`)
 *   2. Native Gemini subagents (`self`, `research`, custom `.md` agents via `LanguageServerService` & `conversation_summaries.db`)
 *
 * Works inside Antigravity's `sidecar_sdk` environment AND supports standalone mode for local testing.
 */

import { spawn } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const BRIDGE_PORT = Number(process.env.AGY_CODEX_BRIDGE_PORT || 18765);
const BRIDGE_BASE = `http://127.0.0.1:${BRIDGE_PORT}`;
const BRIDGE_SCRIPT = resolve(__dirname, '..', '..', 'bin', 'codex_bridge_daemon.py');

function getBridgeToken() {
  if (process.env.AGY_CODEX_BRIDGE_TOKEN) return process.env.AGY_CODEX_BRIDGE_TOKEN.trim();
  const appDir = process.env.AGY_CODEX_APP_DATA_DIR || join(homedir(), '.gemini', 'antigravity-cli');
  const tokPath = join(appDir, 'codex_bridge_token');
  try {
    if (existsSync(tokPath)) return readFileSync(tokPath, 'utf-8').trim();
  } catch {}
  return '';
}

function bridgeHeaders(extra = {}) {
  const tok = getBridgeToken();
  return tok ? { 'X-Bridge-Token': tok, ...extra } : { ...extra };
}

let SidecarAppClass = null;
let ResponseClass = null;

try {
  const sdk = await import('sidecar_sdk');
  SidecarAppClass = sdk.SidecarApp;
  ResponseClass = sdk.Response;
} catch {
  // Standalone fallback class for testing outside sidecar_sdk runtime
  ResponseClass = class Response {
    constructor(body, { contentType = 'text/plain', status = 200, headers = {} } = {}) {
      this.body = body;
      this.contentType = contentType;
      this.status = status;
      this.headers = headers;
    }
  };
  SidecarAppClass = class StandaloneSidecarApp {
    #pages = new Map();
    #apis = new Map();
    get config() {
      return {
        projectId: process.env.ANTIGRAVITY_PROJECT_ID || 'default-cli-project',
        conversationId: process.env.ANTIGRAVITY_CONVERSATION_ID || '',
      };
    }
    page(path, handler) {
      this.#pages.set(path, handler);
    }
    api(path, handler, method = 'POST') {
      if (!this.#apis.has(path)) this.#apis.set(path, new Map());
      this.#apis.get(path).set(method.toUpperCase(), handler);
    }
    run() {
      const port = Number(process.env.ANTIGRAVITY_SIDECAR_WEB_PORT || 18766);
      const server = createServer(async (req, res) => {
        const host = req.headers.host?.split(':')[0];
        if (host !== '127.0.0.1' && host !== 'localhost') {
          res.writeHead(403);
          res.end();
          return;
        }
        const url = new URL(req.url, `http://127.0.0.1:${port}`);
        if (req.method === 'GET' && this.#pages.has(url.pathname)) {
          const html = await this.#pages.get(url.pathname)();
          res.writeHead(200, {
            'Content-Type': 'text/html; charset=utf-8',
            'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; object-src 'none'; base-uri 'self'",
          });
          res.end(html);
          return;
        }
        const methodMap = this.#apis.get(url.pathname);
        if (methodMap && methodMap.has(req.method)) {
          try {
            let data = {};
            if (req.method === 'GET') {
              data = Object.fromEntries(url.searchParams);
            } else {
              const expectedOrigin = `http://${req.headers.host}`;
              if ((req.headers.origin && req.headers.origin !== expectedOrigin) ||
                  !req.headers['content-type']?.toLowerCase().startsWith('application/json')) {
                res.writeHead(403);
                res.end();
                return;
              }
              const chunks = [];
              let size = 0;
              for await (const chunk of req) {
                size += chunk.length;
                if (size > 4 * 1024 * 1024) throw new Error('Request body is too large');
                chunks.push(chunk);
              }
              const raw = Buffer.concat(chunks).toString('utf-8');
              if (raw.trim()) data = JSON.parse(raw);
            }
            const out = await methodMap.get(req.method)(data);
            if (out instanceof ResponseClass) {
              res.writeHead(out.status || 200, {
                'Content-Type': out.contentType || 'text/plain',
                ...(out.headers || {}),
              });
              res.end(out.body);
            } else {
              res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
              res.end(JSON.stringify(out));
            }
          } catch (e) {
            res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
            res.end(JSON.stringify({ error: String(e?.message || e) }));
          }
          return;
        }
        res.writeHead(404);
        res.end('Not found');
      });
      server.listen(port, '127.0.0.1', () => {
        console.log(`[subagent_mission_control] listening on http://127.0.0.1:${port}`);
      });
    }
  };
}

let bridgeStarting = false;

async function ensureBridgeDaemon() {
  try {
    const r = await fetch(`${BRIDGE_BASE}/api/ready`, { signal: AbortSignal.timeout(1200) });
    if (r.ok) return true;
  } catch {}

  if (!bridgeStarting) {
    bridgeStarting = true;
    try {
      const child = spawn('python3', [BRIDGE_SCRIPT, '--port', String(BRIDGE_PORT)], {
        detached: true,
        stdio: 'ignore',
        env: { ...process.env },
      });
      child.on('error', (error) => console.error('[subagent_mission_control] bridge spawn failed:', error));
      child.unref();
    } catch (e) {
      console.error('[subagent_mission_control] failed to spawn bridge daemon:', e);
    } finally {
      setTimeout(() => {
        bridgeStarting = false;
      }, 3000);
    }
  }

  for (let i = 0; i < 15; i++) {
    await new Promise((r) => setTimeout(r, 200));
    try {
      const r = await fetch(`${BRIDGE_BASE}/api/ready`, { signal: AbortSignal.timeout(800) });
      if (r.ok) return true;
    } catch {}
  }
  return false;
}

async function bridgeGet(path, query = {}) {
  await ensureBridgeDaemon();
  const url = new URL(path, BRIDGE_BASE);
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, String(v));
  }
  const resp = await fetch(url, { headers: bridgeHeaders(), signal: AbortSignal.timeout(15000) });
  return await resp.json();
}

async function bridgePost(path, body = {}) {
  await ensureBridgeDaemon();
  const payload = {
    ...body,
    lsAddress: process.env.ANTIGRAVITY_LS_ADDRESS || '',
    csrfToken: process.env.ANTIGRAVITY_CSRF_TOKEN || '',
    agentapiExe: process.env.ANTIGRAVITY_AGENTAPI_EXE || '',
  };
  const resp = await fetch(new URL(path, BRIDGE_BASE), {
    method: 'POST',
    headers: bridgeHeaders({ 'Content-Type': 'application/json' }),
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(25000),
  });
  return await resp.json();
}

// Forward LS credentials immediately on startup
ensureBridgeDaemon().then(() => {
  if (process.env.ANTIGRAVITY_LS_ADDRESS) {
    bridgePost('/api/ls-credentials', {}).catch(() => {});
  }
});

const app = new SidecarAppClass();

const STATIC = {
  '/styles.css': ['styles.css', 'text/css; charset=utf-8'],
  '/app.js': ['app.js', 'application/javascript; charset=utf-8'],
};

app.page('/', () => readFileSync(join(__dirname, 'index.html'), 'utf-8'));

for (const [route, [file, type]] of Object.entries(STATIC)) {
  app.api(route, () => new ResponseClass(readFileSync(join(__dirname, file), 'utf-8'), { contentType: type }), 'GET');
}

app.api('/api/mission-control', async (data) => {
  const rootConv = data.conversationId || app.config.conversationId || '';
  return await bridgeGet('/api/subagents', {
    rootConversationId: rootConv,
    all: data.all ?? 'true',
  });
}, 'GET');

app.api('/api/subagent/inspect', async (data) => {
  return await bridgeGet('/api/subagent/inspect', {
    conversationId: data.conversationId,
  });
}, 'GET');

app.api('/api/subagent/steer', async (data) => {
  return await bridgePost('/api/subagent/steer', data);
}, 'POST');

app.api('/api/subagent/interrupt', async (data) => {
  return await bridgePost('/api/subagent/interrupt', data);
}, 'POST');

app.api('/api/subagent/kill', async (data) => {
  return await bridgePost('/api/subagent/kill', data);
}, 'POST');

app.api('/api/subagent/revert', async (data) => {
  return await bridgePost('/api/subagent/revert', data);
}, 'POST');

app.api('/api/subagent/fork', async (data) => {
  return await bridgePost('/api/subagent/fork', data);
}, 'POST');

app.api('/api/subagent/handoff', async (data) => {
  return await bridgePost('/api/subagent/handoff', data);
}, 'POST');

app.api('/api/subagent/configure', async (data) => {
  return await bridgePost('/api/subagent/configure', data);
}, 'POST');

app.api('/api/subagent/approve', async (data) => {
  return await bridgePost('/api/subagent/approve', data);
}, 'POST');

app.api('/api/subagent/spawn-codex', async (data) => {
  return await bridgePost('/api/dispatch', {
    parentConversationId: data.parentConversationId || app.config.conversationId || '',
    role: data.role || 'Codex Specialist',
    cwd: data.cwd || '',
    model: data.model || undefined,
    effort: data.effort || undefined,
    userInput: data.prompt || '',
    bypassQuotaCheck: Boolean(data.bypassQuotaCheck),
  });
}, 'POST');

app.api('/api/profiles', () => bridgeGet('/api/profiles'), 'GET');
app.api('/api/profiles/define', (data) => bridgePost('/api/profiles/define', data), 'POST');
app.api('/api/profiles/delete', (data) => bridgePost('/api/profiles/delete', data), 'POST');
app.api('/api/codex/threads', (data) => bridgeGet('/api/codex/threads', data), 'GET');
app.api('/api/codex/quota', (data) => bridgeGet('/api/codex/quota', data), 'GET');
app.api('/api/agy/models', () => bridgeGet('/api/agy/models'), 'GET');
app.api('/api/backends', () => bridgeGet('/api/backends'), 'GET');
app.api('/api/backend/models', (data) => bridgeGet('/api/backend/models', data), 'GET');
app.api('/api/codex/adopt', (data) => bridgePost('/api/codex/adopt', {
  parentConversationId: data.parentConversationId || app.config.conversationId || '',
  ...data,
}), 'POST');

app.run();
