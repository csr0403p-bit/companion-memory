#!/usr/bin/env node
/**
 * agent.js — agent-cli：终端唤醒助手
 *
 * 系统提示词 = archive profile 档案卡按 id 拼接（GET /archive/profile?mode=assembled）
 * 对话模型   = SiliconFlow zai-org/GLM-5.2（OpenAI 兼容，SSE 流式 + function calling）
 * MCP 工具   = 服务器各区服务（mcp-base Streamable HTTP，Bearer adminToken，纯 JSON 模式）
 *
 * 用法：
 *   node agent.js                     交互对话（一行一条）
 *   cat msg.txt | node agent.js       管道：整段输入作为一条消息（多行不拆）
 *
 * 指令（交互模式）：/new 清空对话  /profile 看提示词  /tools 看已挂工具  /exit 退出
 *
 * 注意：路径与 token 全部可在 config.json 覆盖（adminToken 直配，或经 archiveConfig 读 archive-mcp 的 config）
 */
'use strict';
const fs = require('node:fs');
const readline = require('node:readline');

const HOME = '/home/agent';
const MCP_PROTO = '2025-06-18';

// 同目录 config.json 可覆盖任意默认项
const CFG = Object.assign(
  {
    archiveBase: 'http://127.0.0.1:3012/archive',
    archiveConfig: `${HOME}/archive-mcp/config.json`, // adminToken 单一来源（MCP 也用它）
    llmUrl: 'https://api.siliconflow.cn/v1/chat/completions',
    model: 'zai-org/GLM-5.2',
    apiKeyPath: `${HOME}/.secrets/siliconflow-api-key`,
    // maxTokens 默认不设——用模型自身输出上限；config.json 显式给 maxTokens 才生效
    maxHistory: 40, // 对话上限（不含 system），超出按完整交互块裁最旧的
    maxToolRounds: 8, // 一次提问最多连续工具往返轮数
    toolResultCap: 30000, // 单个工具结果注入上下文的字数上限（中间截断）
    mcpServers: [],
  },
  (() => {
    try { return JSON.parse(fs.readFileSync(`${__dirname}/config.json`, 'utf8')); }
    catch { return {}; }
  })()
);

const out = (...a) => process.stdout.write(a.join(''));

function loadSecrets() {
  const adminToken = CFG.adminToken || JSON.parse(fs.readFileSync(CFG.archiveConfig, "utf8")).adminToken; // config 直配优先，否则读 archive-mcp 的 config
  const apiKey = fs.readFileSync(CFG.apiKeyPath, 'utf8').trim();
  if (!adminToken || !apiKey) throw new Error('adminToken 或 api key 为空');
  return { adminToken, apiKey };
}

async function fetchProfile(adminToken) {
  const res = await fetch(`${CFG.archiveBase}/profile?mode=assembled`, {
    headers: { Authorization: `Bearer ${adminToken}` },
  });
  if (!res.ok) throw new Error(`profile 拉取失败 HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
  const j = await res.json();
  if (!j.ok || !j.prompt) throw new Error(`profile 返回异常: ${JSON.stringify(j).slice(0, 200)}`);
  return j.prompt;
}

// ━━━ MCP 客户端（Streamable HTTP 纯 JSON 模式；对 mcp-base 与 gateway 同款协议）━━━
class McpConn {
  constructor(name, url, token) {
    this.name = name;
    this.url = url;
    this.token = token;
    this.sid = null;
    this.rpcId = 0;
    this.tools = [];
  }

  // 超时：控制类调用 10s；notify 5s（qq-agent-mcp 不回 notification，必须快断）；tools/call 60s
  async rpc(method, params, timeoutMs = 10000) {
    const id = ++this.rpcId;
    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json',
      Authorization: `Bearer ${this.token}`,
    };
    if (this.sid) headers['Mcp-Session-Id'] = this.sid;
    const res = await fetch(this.url, {
      method: 'POST', headers,
      body: JSON.stringify({ jsonrpc: '2.0', id, method, params: params ?? {} }),
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${(await res.text().catch(() => '')).slice(0, 120)}`);
    const sid = res.headers.get('mcp-session-id');
    if (sid) this.sid = sid;
    const j = await res.json().catch(() => null);
    if (!j || j.error) throw new Error(j?.error?.message || '空响应');
    return j.result;
  }

  async notify(method) {
    // notification 本就允许对端只回 202 甚至不回（qq-agent-mcp 就不回）——超时即放弃，不算错
    const res = await fetch(this.url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json', Accept: 'application/json',
        Authorization: `Bearer ${this.token}`, ...(this.sid ? { 'Mcp-Session-Id': this.sid } : {}),
      },
      body: JSON.stringify({ jsonrpc: '2.0', method }),
      signal: AbortSignal.timeout(5000),
    }).catch(() => null);
    if (res && !res.ok && res.status !== 202) throw new Error(`HTTP ${res.status}`);
  }

  async init() {
    await this.rpc('initialize', {
      protocolVersion: MCP_PROTO, capabilities: {},
      clientInfo: { name: 'agent-cli', version: '1.0' },
    });
    try { await this.notify('notifications/initialized'); } catch { /* 部分实现不回 202，不致命 */ }
    return this;
  }

  async listTools() {
    const r = await this.rpc('tools/list', {});
    this.tools = r?.tools ?? [];
    return this.tools;
  }

  async call(name, args) {
    const r = await this.rpc('tools/call', { name, arguments: args }, 60000);
    const parts = r?.content ?? [];
    let text = '';
    for (const p of parts) {
      if (p.type === 'text') text += p.text;
      else if (p.type === 'image') text += '\n[图片内容，终端不显示]';
    }
    if (r?.isError) text = `[工具报错] ${text}`;
    return text || JSON.stringify(r);
  }

  // call + JSON.parse——编排器（night.js）把工具返回当结构化数据用时的入口
  async callJson(name, args) {
    const text = await this.call(name, args);
    try {
      const j = JSON.parse(text);
      return typeof j === 'object' && j !== null ? j : { ok: false, error: `非对象返回: ${text.slice(0, 120)}` };
    } catch {
      return { ok: false, error: `返回非 JSON: ${text.slice(0, 200)}` };
    }
  }
}

// 连接所有配置的 MCP 服务器，聚合成 OpenAI tools + 注册表；单个失败不拖垮整体
async function attachMcpServers(adminToken) {
  const token = CFG.mcpToken || adminToken;
  const registry = new Map(); // toolName -> McpConn
  const conns = [];
  const tools = [];
  for (const s of CFG.mcpServers ?? []) {
    if (s.enabled === false) continue;
    try {
      const conn = new McpConn(s.name, s.url, s.token || token);
      await conn.init();
      const list = await conn.listTools();
      let n = 0;
      for (const t of list) {
        if (!/^[A-Za-z0-9_-]{1,64}$/.test(t.name)) continue;
        if (registry.has(t.name)) continue; // 跨区重名，保先注册的（各区已按前缀命名，基本不撞）
        registry.set(t.name, conn);
        tools.push({
          type: 'function',
          function: {
            name: t.name,
            description: (t.description || '').slice(0, 2000),
            parameters: t.inputSchema && typeof t.inputSchema === 'object' ? t.inputSchema : { type: 'object', properties: {} },
          },
        });
        n++;
      }
      conns.push(conn);
      out(`  ✓ ${s.name}（${n} 工具）\n`);
    } catch (e) {
      out(`  ✗ ${s.name}：${e.message}（跳过）\n`);
    }
  }
  return { conns, tools, registry };
}

async function executeTool(registry, name, argsJson) {
  const conn = registry.get(name);
  if (!conn) return `[没有这个工具: ${name}]`;
  let args;
  try { args = JSON.parse(argsJson || '{}'); }
  catch { args = {}; }
  let text;
  try { text = await conn.call(name, args); }
  catch (e) { return `[调用失败] ${e.message}`; }
  if (text.length > CFG.toolResultCap) {
    const head = text.slice(0, CFG.toolResultCap / 2);
    const tail = text.slice(-CFG.toolResultCap / 4);
    text = `${head}\n…[中间截断，原文 ${text.length} 字]…\n${tail}`;
  }
  return text;
}

// ━━━ LLM 流式（正文实时打印；tool_calls 增量累积；usage 累计给 night.js 记账）━━━
const tokenUsage = { prompt: 0, completion: 0, total: 0, cached: 0, calls: 0 };
function resetTokenUsage() { tokenUsage.prompt = 0; tokenUsage.completion = 0; tokenUsage.total = 0; tokenUsage.cached = 0; tokenUsage.calls = 0; }
async function chatStream(messages, apiKey, tools) {
  const body = { model: CFG.model, messages, stream: true, stream_options: { include_usage: true } };
  if (CFG.maxTokens != null) body.max_tokens = CFG.maxTokens; // 默认不传——模型自己管
  if (tools && tools.length) body.tools = tools;
  const res = await fetch(CFG.llmUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify(body),
  });
  if (!res.ok || !res.body) {
    const t = await res.text().catch(() => '');
    throw new Error(`LLM HTTP ${res.status}: ${t.slice(0, 300)}`);
  }
  tokenUsage.calls++;
  const decoder = new TextDecoder();
  let buf = '';
  let text = '';
  let reasoningLen = 0;
  let lastUsage = null; // 本请求的 usage 快照——服务端可能在多个 chunk 都带，只认最后一份
  const tcAcc = new Map(); // index -> {id, name, args}
  for await (const chunk of res.body) {
    buf += decoder.decode(chunk, { stream: true });
    const lines = buf.split('\n');
    buf = lines.pop();
    for (const line of lines) {
      const s = line.trim();
      if (!s.startsWith('data:')) continue;
      const payload = s.slice(5).trim();
      if (payload === '[DONE]') continue;
      try {
        const j = JSON.parse(payload);
        const d = j.choices?.[0]?.delta;
        if (j.usage) lastUsage = j.usage; // 覆盖——流结束时只按最后快照计一次
        if (!d) continue;
        if (d.reasoning_content) reasoningLen += d.reasoning_content.length;
        if (d.content) { text += d.content; out(d.content); }
        if (Array.isArray(d.tool_calls)) {
          for (const tc of d.tool_calls) {
            const i = tc.index ?? 0;
            const acc = tcAcc.get(i) ?? { id: '', name: '', args: '' };
            if (tc.id) acc.id = tc.id;
            if (tc.function?.name) acc.name += tc.function.name;
            if (tc.function?.arguments) acc.args += tc.function.arguments;
            tcAcc.set(i, acc);
          }
        }
      } catch { /* 非 JSON 行忽略 */ }
    }
  }
  if (lastUsage) { // 流结束：按最后一份快照计一次，杜绝多 chunk 重复累计
    tokenUsage.prompt += lastUsage.prompt_tokens || 0;
    tokenUsage.completion += lastUsage.completion_tokens || 0;
    tokenUsage.total += lastUsage.total_tokens || 0;
    const cached = lastUsage.prompt_tokens_details?.cached_tokens ?? lastUsage.prompt_cache_hit_tokens ?? 0;
    tokenUsage.cached += cached || 0; // 前缀缓存命中（命中部分计费打折）
  }
  const toolCalls = [...tcAcc.keys()].sort((a, b) => a - b).map((i) => tcAcc.get(i));
  return { text, reasoningLen, toolCalls, promptTokens: lastUsage?.prompt_tokens ?? 0 };
}

// 一轮完整提问：LLM ↔ 工具往返，直到给出正文或轮数用尽（night.js 用 maxRounds 收紧查证轮数）
async function sendTurn(messages, apiKey, mcp, maxRounds) {
  const rounds = maxRounds ?? CFG.maxToolRounds;
  for (let round = 0; round <= rounds; round++) {
    const useTools = mcp.tools.length > 0 && round < rounds ? mcp.tools : null;
    const { text, reasoningLen, toolCalls } = await chatStream(messages, apiKey, useTools);
    if (!toolCalls.length) {
      return { text, reasoningLen };
    }
    messages.push({
      role: 'assistant',
      content: text || '',
      tool_calls: toolCalls.map((tc) => ({
        id: tc.id, type: 'function',
        function: { name: tc.name, arguments: tc.args || '{}' },
      })),
    });
    for (const tc of toolCalls) {
      const brief = tc.args.length > 100 ? `${tc.args.slice(0, 100)}…` : tc.args;
      out(`\n  [工具] ${tc.name}(${brief})`);
      const result = await executeTool(mcp.registry, tc.name, tc.args);
      out(` → ${result.includes('[调用失败]') || result.includes('[没有这个工具]') || result.includes('[工具报错]') ? '失败' : `${result.length} 字`}`);
      messages.push({ role: 'tool', tool_call_id: tc.id, content: result });
    }
    out('\n  [思考中…]');
  }
  // 轮数用尽：不给工具，逼出最终回答
  const { text } = await chatStream(messages, apiKey, null);
  return { text, reasoningLen: 0 };
}

// 裁历史：从最旧处按「user 起、下一 user 前」的完整交互块删，不把 tool 消息切成孤儿
function trimHistory(messages, cap) {
  while (messages.length > cap + 1) {
    let i = 1;
    while (i < messages.length && messages[i].role !== 'user') i++;
    const cutEnd = Math.min(i + 1, messages.length); // 含这条 user
    messages.splice(1, cutEnd - 1);
  }
}

// 管道模式：一次性读完整个 stdin（多行合并为一条消息，避免逐行 await 时丢行）
function readAllStdin() {
  return new Promise((res) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (c) => { data += c; });
    process.stdin.on('end', () => res(data));
    process.stdin.on('error', () => res(data));
  });
}

async function main() {
  out(`—— agent-cli：唤醒助手（${CFG.model}）——\n`);
  const { adminToken, apiKey } = loadSecrets();
  const profile = await fetchProfile(adminToken);
  out(`[系统提示词已注入：${profile.length} 字档案卡]\n`);

  out('[连接 MCP 分区…]\n');
  const mcp = await attachMcpServers(adminToken);
  out(`[MCP 就绪：${mcp.conns.length} 个分区 / ${mcp.tools.length} 个工具]\n`);

  // ── 管道模式：整段输入 = 一条消息，一问一答后退出 ──
  if (!process.stdin.isTTY) {
    const text = (await readAllStdin()).replace(/\r\n/g, '\n').trim();
    if (!text) { out('[没有输入]\n[再见]\n'); return; }
    const messages = [
      { role: 'system', content: profile },
      { role: 'user', content: text },
    ];
    out('\n助手> ');
    try {
      const { text: reply, reasoningLen } = await sendTurn(messages, apiKey, mcp);
      if (!reply.trim()) {
        out(reasoningLen > 0
          ? `\n[只有思考没有正文——max_tokens 可能被思考吃光，可调大 config.json 的 maxTokens]\n`
          : '\n[空回复]\n');
      }
    } catch (e) {
      out(`\n[出错：${e.message}]\n`);
    }
    out('[再见]\n');
    return;
  }

  // ── 交互模式：一行一条 ──
  out('[指令：/new 清空对话  /profile 看提示词  /tools 看已挂工具  /exit 退出]\n');
  let messages = [{ role: 'system', content: profile }];
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: false });
  const pending = [];
  let closed = false;
  let waiter = null;
  rl.on('line', (l) => { waiter ? waiter(l) : pending.push(l); });
  rl.on('close', () => { closed = true; if (waiter) waiter(null); });
  const nextLine = () => new Promise((res) => {
    if (pending.length) return res(pending.shift());
    if (closed) return res(null);
    waiter = (v) => { waiter = null; res(v); };
  });

  for (;;) {
    out('\n你> ');
    const line = (await nextLine())?.trim();
    if (line === undefined || line === null) break;
    if (!line) continue;
    if (line === '/exit' || line === '/quit') break;
    if (line === '/new') { messages = [{ role: 'system', content: profile }]; out('[对话已清空，档案卡仍在]\n'); continue; }
    if (line === '/profile') { out(`\n——当前系统提示词（${profile.length} 字）——\n${profile.slice(0, 500)}…\n`); continue; }
    if (line === '/tools') {
      out('\n——已挂 MCP 分区——\n');
      for (const c of mcp.conns) out(`  ${c.name}（${c.tools.length} 工具）\n`);
      out(`  合计 ${mcp.tools.length} 个\n`);
      continue;
    }

    const startLen = messages.length;
    messages.push({ role: 'user', content: line });
    try {
      out('\n助手> ');
      const { text, reasoningLen } = await sendTurn(messages, apiKey, mcp);
      if (!text.trim()) {
        messages.length = startLen;
        out(reasoningLen > 0
          ? '\n[只有思考没有正文——max_tokens 可能被思考吃光，可调大 config.json 的 maxTokens]\n'
          : '\n[空回复，未计入对话]\n');
        continue;
      }
      trimHistory(messages, CFG.maxHistory);
      out('\n');
    } catch (e) {
      messages.length = startLen; // 整轮回滚（含半程 tool 消息），这条不算数
      out(`\n[出错：${e.message}\n这条没计入对话，直接重发即可]\n`);
    }
  }
  out('\n[再见]\n');
}

// 供 night.js 复用（夜间整理：同一套 profile 注入 + MCP 客户端 + 工具回环）
module.exports = { CFG, loadSecrets, fetchProfile, McpConn, chatStream, sendTurn, tokenUsage, resetTokenUsage };

if (require.main === module) {
  main().catch((e) => { console.error('[致命]', e.stack || e.message); process.exit(1); });
}
