#!/usr/bin/env node
/**
 * embed-server.js —— 极简 embedding 服务（API 代理版，零依赖）
 *
 * 实现 server.js / backfill.js 需要的 POST /embed 契约，后端转发给任意
 * OpenAI 兼容的 /embeddings API（硅基流动 / OpenAI / 自建 vLLM 都行）。
 * 不会本地跑模型，适合不想装 GPU 环境的场景。
 *
 * 用法：
 *   EMBED_API_KEY=sk-xxx node embed-server.js
 *   # 可选环境变量：
 *   #   PORT           监听端口（默认 3013，与 config.json 的 embedPort 对应）
 *   #   EMBED_API_URL  embeddings 端点（默认硅基流动）
 *   #   EMBED_MODEL    模型名（默认 Qwen/Qwen3-Embedding-0.6B，1024 维）
 *
 * ⚠️ 向量表固定 1024 维（vec0 embedding float[1024]）：换模型必须挑 1024 维的，
 *    否则这里会直接报错拒绝写入形状不对的向量。
 */
'use strict';

const http = require('node:http');

const PORT = Number(process.env.PORT || 3013);
const API_URL = process.env.EMBED_API_URL || 'https://api.siliconflow.cn/v1/embeddings';
const MODEL = process.env.EMBED_MODEL || 'Qwen/Qwen3-Embedding-0.6B';
const API_KEY = process.env.EMBED_API_KEY || '';
const BATCH = 8;          // 与 server.js 的 EMBED_BATCH 一致，小批量防超时
const DIM = 1024;         // vec 表结构固定维度
const TIMEOUT_MS = 60000;

if (!API_KEY) {
  console.warn('[embed-server] 未设置 EMBED_API_KEY——服务会启动，但所有 /embed 请求都会失败');
}

function l2normalize(vec) {
  let sum = 0;
  for (const x of vec) sum += x * x;
  const n = Math.sqrt(sum);
  return n > 0 ? vec.map((x) => x / n) : vec;
}

async function embedBatch(texts) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${API_KEY}` },
      body: JSON.stringify({ model: MODEL, input: texts, encoding_format: 'float' }),
      signal: ctl.signal,
    });
    const j = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`API ${res.status}: ${j.error?.message || j.message || res.statusText}`);
    const data = (j.data ?? []).slice().sort((a, b) => a.index - b.index);
    if (data.length !== texts.length) throw new Error(`API 返回 ${data.length} 条，预期 ${texts.length}`);
    return data.map((d) => {
      const v = d.embedding;
      if (!Array.isArray(v) || v.length !== DIM) {
        throw new Error(`模型 ${MODEL} 输出 ${v?.length} 维，向量表固定 ${DIM} 维——请换 1024 维模型，或改 server.js/backfill.js 里的 vec0 维度后重建向量表`);
      }
      return l2normalize(v);
    });
  } finally {
    clearTimeout(timer);
  }
}

const server = http.createServer(async (req, res) => {
  const send = (status, obj) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); };
  if (req.method === 'GET' && req.url === '/healthz') {
    return send(200, { ok: true, model: MODEL, api: API_URL, key_set: Boolean(API_KEY) });
  }
  if (req.method === 'POST' && req.url === '/embed') {
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', async () => {
      try {
        const { texts } = JSON.parse(body || '{}');
        if (!Array.isArray(texts) || texts.length === 0 || typeof texts[0] !== 'string') {
          return send(400, { ok: false, error: 'body 应为 {"texts": ["...", ...]}' });
        }
        const vectors = [];
        for (let i = 0; i < texts.length; i += BATCH) {
          vectors.push(...await embedBatch(texts.slice(i, i + BATCH)));
        }
        return send(200, { ok: true, vectors });
      } catch (e) {
        return send(502, { ok: false, error: String(e?.message ?? e) });
      }
    });
    return;
  }
  send(404, { ok: false, error: 'not found（只有 GET /healthz 和 POST /embed）' });
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`embed-server listening on 127.0.0.1:${PORT}  model=${MODEL}  api=${API_URL}`);
});
