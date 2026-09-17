// mcp-base —— MCP/REST 微服务共享底座（2026-08-20；已内置到本仓库）
//
// 用途：第二批搬迁的 5 个区服务（timeline/rule/clippings/continuity/task-mcp）共用。
// 部署拓扑仍是独立服务（各自目录/端口/systemd/隧道），只是 HTTP+MCP+鉴权骨架共用这一份。
// diary-mcp（第一批）当时自带骨架，保持不动。
//
// 提供：
//   · MCP Streamable HTTP 传输层（对齐云端 mcp.ts：initialize 回会话、notifications 202、
//     GET=SSE ping 流 60s 关、Accept SSE 时 POST 包 event 帧、DELETE 注销；会话内存 30 分钟 TTL）
//   · Bearer 鉴权（appToken / adminToken，双钥匙模型与云端一致）
//   · JSON 文件存储（KV 形状：readJSON/writeJSON/getArr/setArr，原子替换）
//   · SQLite 存储（D1 形状：openDb，node:sqlite DatabaseSync）
//
// 用法（各服务 server.js）：
//   const { startService, jsonStore } = require('./mcp-base.js');
//   startService({
//     name: 'agent-timeline', port: 3005, basePath: '/timeline',
//     tools: [...], callTool, rest: handleRest, dataDir: './data',
//   });

'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const PROTOCOL_VERSION = '2025-06-18';
const SESSION_TTL_MS = 30 * 60 * 1000;

// ━━━ JSON 文件存储（KV 形状，key 的 : 换成 _ 做文件名）━━━
function makeJsonStore(dataDir) {
  fs.mkdirSync(dataDir, { recursive: true });
  const keyToFile = (kvKey) => path.join(dataDir, kvKey.replace(/:/g, '_') + '.json');
  return {
    keyToFile,
    readJSON(kvKey, fallback = null) {
      try { return JSON.parse(fs.readFileSync(keyToFile(kvKey), 'utf8')); }
      catch { return fallback; }
    },
    writeJSON(kvKey, value) {
      const f = keyToFile(kvKey);
      const tmp = f + '.tmp-' + crypto.randomBytes(4).toString('hex');
      fs.writeFileSync(tmp, JSON.stringify(value), 'utf8');
      fs.renameSync(tmp, f);
    },
    deleteKey(kvKey) {
      try { fs.unlinkSync(keyToFile(kvKey)); } catch {}
    },
    getArr(kvKey, fallback = []) {
      return this.readJSON(kvKey, fallback);
    },
    setArr(kvKey, arr) {
      this.writeJSON(kvKey, arr);
    },
  };
}

// ━━━ SQLite 存储（D1 形状）━━━
function openDb(dbFile, opts = {}) {
  const { DatabaseSync } = require('node:sqlite');
  fs.mkdirSync(path.dirname(dbFile), { recursive: true });
  // allowExtension：archive-mcp 需要加载 sqlite-vec；其他服务默认不开（保持原状）
  const db = new DatabaseSync(dbFile, opts.allowExtension ? { allowExtension: true } : {});
  db.exec('PRAGMA journal_mode = WAL;');
  return db;
}

// ━━━ 时间工具（对齐云端 lib.ts）━━━
function nowMs() { return Date.now(); }
function fmtTs(ms) {
  return new Date(ms).toLocaleString('sv-SE', { timeZone: 'Asia/Shanghai' }).slice(0, 16);
}
function fmtTsSec(ms) {
  return new Date(ms).toLocaleString('sv-SE', { timeZone: 'Asia/Shanghai' }).slice(0, 19);
}
function todayCN() {
  return new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Shanghai' });
}

// ━━━ 传输层（对齐云端 src/mcp.ts）━━━
const JSON_HEADERS = {
  'Content-Type': 'application/json',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Expose-Headers': 'Mcp-Session-Id, MCP-Protocol-Version',
  'MCP-Protocol-Version': PROTOCOL_VERSION,
};
const SSE_HEADERS = {
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache',
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Expose-Headers': 'Mcp-Session-Id, MCP-Protocol-Version',
  'MCP-Protocol-Version': PROTOCOL_VERSION,
};

function sendJson(res, status, data, extraHeaders) {
  res.writeHead(status, { ...JSON_HEADERS, ...extraHeaders });
  res.end(JSON.stringify(data));
}
function jsonOk(res, data) { sendJson(res, 200, data); }
function jsonErr(res, msg, status = 400) { sendJson(res, status, { ok: false, error: msg }); }

function readBody(req, limit = 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));  // 返回原始 Buffer，二进制安全
    req.on('error', reject);
  });
}

// ━━━ 服务装配 ━━━
// opts: {
//   name     服务器信息名（initialize 回包用）
//   port     监听端口
//   basePath 区前缀，如 '/timeline'（REST 和 MCP 都挂在它下面，与云端路径一致）
//   appToken / adminToken
//   tools    MCP 工具数组
//   callTool(toolName, args) -> 数据对象（返回体自动包成 text content）
//   rest({ url, subpath, method, body, auth, req }) -> { status, json } | { __stream: (res)=>void } | null   null=404
//   maxBody  可选，请求体上限字节（默认 1MB；archive 区 import 大 json 需要 32MB）
//   preInit() 可选，服务启动时执行（建表等）
// }
function startService(opts) {
  const cfg = {
    name: opts.name || 'agent-service',
    port: Number(process.env.PORT || opts.port || 3004),
    basePath: opts.basePath,
    appToken: opts.appToken,
    adminToken: opts.adminToken,
    tools: opts.tools || [],
    callTool: opts.callTool,
    rest: opts.rest,
    preInit: opts.preInit,
    maxBody: opts.maxBody || 1024 * 1024,
  };

  const sessions = new Map(); // sid -> 最后活跃时间戳

  const isAdmin = (auth) => auth === `Bearer ${cfg.adminToken}`;
  const isAny = (auth) => auth === `Bearer ${cfg.adminToken}` || auth === `Bearer ${cfg.appToken}`;

  function mcpSseFrame(res, jsonText, extraHeaders) {
    res.writeHead(200, { ...SSE_HEADERS, ...extraHeaders });
    res.end(`event: message\ndata: ${jsonText}\n\n`);
  }

  function openEventStream(req, res) {
    res.writeHead(200, SSE_HEADERS);
    res.flushHeaders(); // Node 的 writeHead 只排队不发头，SSE 必须立刻发
    const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch { clearInterval(ping); } }, 15000);
    const closer = setTimeout(() => { clearInterval(ping); try { res.end(); } catch {} }, 60000);
    req.on('close', () => { clearInterval(ping); clearTimeout(closer); });
  }

  async function handleMcpPost(req, res) {
    let body;
    try { body = JSON.parse((await readBody(req, cfg.maxBody)).toString('utf8')); }
    catch { sendJson(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } }); return; }

    const rpcMethod = body?.method;
    const rpcId = body?.id;
    const accept = req.headers.accept || '';
    const wantSse = accept.includes('text/event-stream');
    const sessionId = req.headers['mcp-session-id'] || '';

    if (typeof rpcMethod === 'string' && rpcMethod.startsWith('notifications/')) {
      res.writeHead(202, JSON_HEADERS);
      res.end();
      return;
    }

    if (rpcMethod === 'initialize') {
      const sid = crypto.randomUUID();
      sessions.set(sid, Date.now());
      const inner = JSON.stringify({
        jsonrpc: '2.0', id: rpcId,
        result: { protocolVersion: PROTOCOL_VERSION, capabilities: { tools: {} }, serverInfo: { name: cfg.name, version: '1.0.0' } },
      });
      if (wantSse) mcpSseFrame(res, inner, { 'Mcp-Session-Id': sid });
      else sendJson(res, 200, JSON.parse(inner), { 'Mcp-Session-Id': sid });
      return;
    }

    if (sessionId) {
      const last = sessions.get(sessionId);
      if (!last || Date.now() - last > SESSION_TTL_MS) {
        sendJson(res, 400, { jsonrpc: '2.0', id: rpcId, error: { code: -32600, message: 'Invalid session' } });
        return;
      }
      sessions.set(sessionId, Date.now());
    }

    let result;
    if (rpcMethod === 'tools/list') {
      result = { tools: cfg.tools };
    } else if (rpcMethod === 'tools/call') {
      const toolName = body.params?.name;
      const args = body.params?.arguments ?? {};
      let data;
      try { data = await cfg.callTool(toolName, args); }
      catch (e) { data = { ok: false, error: String(e?.message ?? e) }; }
      // callTool 返回 string = 原样文本（shelf 区的工具就是纯文本回答）；其他返回对象 → JSON 文本
      // 特殊形状 { __mcp_text, __mcp_image: { base64, mimeType } } → text + image content（图片附件用）
      if (typeof data === 'string') {
        result = { content: [{ type: 'text', text: data }] };
      } else if (data && typeof data === 'object' && data.__mcp_image) {
        const content = [];
        if (data.__mcp_text !== undefined) {
          content.push({ type: 'text', text: typeof data.__mcp_text === 'string' ? data.__mcp_text : JSON.stringify(data.__mcp_text) });
        }
        content.push({ type: 'image', data: data.__mcp_image.base64, mimeType: data.__mcp_image.mimeType });
        result = { content };
      } else {
        result = { content: [{ type: 'text', text: JSON.stringify(data) }] };
      }
    } else {
      sendJson(res, 200, { jsonrpc: '2.0', id: rpcId, error: { code: -32601, message: 'Method not found' } });
      return;
    }
    const text = JSON.stringify({ jsonrpc: '2.0', id: rpcId, result });
    if (wantSse) mcpSseFrame(res, text);
    else { res.writeHead(200, JSON_HEADERS); res.end(text); }
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const pathname = url.pathname;
    const method = req.method;
    const auth = req.headers.authorization || '';

    if (method === 'OPTIONS') {
      res.writeHead(200, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,PATCH,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, Mcp-Session-Id, MCP-Protocol-Version',
      });
      res.end();
      return;
    }

    if (pathname === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, ts: nowMs() }));
      return;
    }

    const base = cfg.basePath;
    if (pathname === base || pathname.startsWith(base + '/')) {
      const subpath = pathname.slice(base.length) || '/';

      // MCP：POST/GET/DELETE {base}/mcp —— 只认 ADMIN（对齐云端）
      if (subpath === '/mcp' && ['POST', 'GET', 'DELETE'].includes(method)) {
        if (!isAdmin(auth)) return jsonErr(res, 'unauthorized', 401);
        if (method === 'GET') return openEventStream(req, res);
        if (method === 'DELETE') {
          const sid = req.headers['mcp-session-id'] || '';
          if (sid) sessions.delete(sid);
          res.writeHead(200, JSON_HEADERS);
          res.end();
          return;
        }
        return handleMcpPost(req, res).catch((e) => jsonErr(res, `internal: ${e.message}`, 500));
      }

      // REST：交给各区自己的 handler（raw=原始 Buffer，二进制上传用；body 仍是 utf8 串，兼容旧服务）
      return (async () => {
        try {
          const raw = await readBody(req, cfg.maxBody);
          const out = await cfg.rest({ url, subpath, method, body: raw.toString('utf8'), raw, auth, req });
          if (!out) return jsonErr(res, 'not found', 404);
          if (out.__stream) return await out.__stream(res);
          sendJson(res, out.status || 200, out.json);
        } catch (e) {
          if (e instanceof SyntaxError) return jsonErr(res, 'invalid json body');
          if (e.message === 'body too large') return jsonErr(res, 'payload too large', 413);
          return jsonErr(res, String(e?.message ?? e), 400);
        }
      })();
    }

    return jsonErr(res, 'not found', 404);
  });

  if (cfg.preInit) cfg.preInit();

  server.listen(cfg.port, '127.0.0.1', () => {
    console.log(`${cfg.name} listening on 127.0.0.1:${cfg.port}`);
  });

  return { isAdmin, isAny, server };
}

module.exports = { startService, makeJsonStore, openDb, nowMs, fmtTs, fmtTsSec, todayCN };
