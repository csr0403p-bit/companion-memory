// archive-mcp —— 第七区「我们曾经」（2026-08-22 阶段2：服务器全功能版，脱离云端独立运行）
//
// 从云端 agent-core /archive 区全量移植（src/regions/archive.ts 2026-08-22 版）：
//   · 38 个 MCP 工具（29 个与云端对齐 + 本地新增：subgraph / network_stats / profile / associate / event_path / night 系列）+ 全部 REST 路径
//   · 事件 CRUD + 手动边 + 网络重建（rebuild-network，Louvain 社区）—— qwen 本地 embedding
//   · 因果反思层（probe/review/chain、focus 队列、批量判定、判定回声）、情感轴、梦境、记忆归纳
//   · 2026-09-14 AI 侧增强：archive_event_subgraph（子图抽取）、archive_network_stats（桥边/割点）
//   · 2026-09-16 联想层（参照 HippoRAG pattern completion / RRP 路径评分）：
//     archive_associate（语义种子 → 事件网 PPR 扩散）、archive_event_path（A→B 有界枚举 + 可靠性评分）
// 存储：SQLite（承接 D1 ARCHIVE_DB 全表）+ data/raw/{uuid}.json（承接 R2）+ sqlite-vec（承接 Vectorize）
// 向量：本地 embed-server（127.0.0.1:3013，Qwen3-Embedding-0.6B，1024 维，归一化）
// 鉴权与云端一致：REST = APP 或 ADMIN；MCP = 仅 ADMIN。
//
// 与云端实现差异（有意为之）：
//   1. embedding：bge-m3(云端 AI) → qwen3-emb-0.6b(本地 CPU)。批量 8（embed-server 单线程实测）
//      记忆向量 rowid=1000000+idx；事件向量 rowid=eventId*1000(+fragIdx+1)
//   3. thematic 边阈值：云端 bge cosine 0.65 → 本地 qwen cosine（config.json thematicThreshold，
//      默认 0.55，可校准）。qwen 语义相关文本的余弦≈0.5-0.7，同文本≈1.0
//   4. R2 → data/raw/{uuid}.json

'use strict';

const path = require('node:path');
const fs = require('node:fs');
const { startService, openDb, nowMs, fmtTs } = require('./mcp-base.js');
const { louvain } = require('./louvain.js');

// ━━━ 常量（对齐云端 archive.ts）━━━

const EMBED_BATCH = 8;          // 单次发给 embed-server 的条数（qwen CPU 单线程 + BATCH=8 是回填实测值）
const EMBED_QUERY_MAX = 2500;
const EVENT_ROWID_SCALE = 1000;      // 事件向量 rowid = eventId*1000 + (whole?0:fragIdx+1)
const BM25_K1 = 1.5;                 // 事件混合检索 BM25 饱和参数
const BM25_B = 0.75;                 // 事件混合检索 BM25 长度归一参数
const EMBED_TIMEOUT_MS = 120000;
const EMBED_RETRY = 3;
const EDGE_WEIGHTS = { causal: 3.0, thematic: 1.5, overlap: 1.0 };
const PROBE_DEFAULT_LIMIT = 5;
const PROBE_MAX_LIMIT = 10;
const SUMMARY_CLIP_LEN = 120;
const EVENT_TITLE_BATCH = 90;

const CFG = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8'));
for (const k of ['appToken', 'adminToken']) {
  if (!CFG[k] || String(CFG[k]).startsWith('CHANGE_ME')) {
    console.warn(`[config] ${k} 还是占位符——请在 config.json 里换成你自己的随机串（任何人拿到它即可访问你的数据）`);
  }
}
const PORT = Number(process.env.PORT || CFG.port || 3012);
const DATA_DIR = path.resolve(__dirname, CFG.dataDir || 'data');
const THEMATIC_THRESHOLD = Number(CFG.thematicThreshold ?? 0.748);  // qwen 余弦尺度（用户定: ~2.4 条语义边/事件 ≈ 535 条）
const REBUILD_LOG = CFG.rebuildLog || path.join(DATA_DIR, 'rebuild-network.log');

const DB = openDb(path.join(DATA_DIR, 'archive.sqlite'), { allowExtension: true });

// ━━━ Schema（自动建库；已存在的库跳过）━━━
DB.exec(`
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, summary TEXT,
  time_span_start TEXT NOT NULL, time_span_end TEXT, themes TEXT, source_block_ids TEXT,
  importance REAL DEFAULT 0.5, event_type TEXT DEFAULT 'moment', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  vector_dirty INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS event_edges (
  event_a INTEGER NOT NULL, event_b INTEGER NOT NULL, edge_type TEXT NOT NULL,
  detail TEXT, confidence REAL NOT NULL DEFAULT 1.0, created_at TEXT NOT NULL,
  PRIMARY KEY (event_a, event_b, edge_type)
);
CREATE TABLE IF NOT EXISTS event_communities (
  id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL DEFAULT '', note TEXT,
  member_ids TEXT NOT NULL, core_event_id INTEGER, internal_edges INTEGER DEFAULT 0,
  time_span_start TEXT, time_span_end TEXT, edge_type TEXT NOT NULL DEFAULT 'all',
  renamed INTEGER NOT NULL DEFAULT 0, created_at TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS causal_reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL, candidate_id INTEGER,
  verdict TEXT NOT NULL, reason TEXT, decided_by TEXT, decided_at TEXT NOT NULL, defer_until TEXT
);
CREATE TABLE IF NOT EXISTS profile (
  title TEXT PRIMARY KEY, content TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS night_switch (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  enabled INTEGER NOT NULL DEFAULT 1,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS night_reports (
  date TEXT PRIMARY KEY,
  verdicts_total INTEGER NOT NULL DEFAULT 0,
  link INTEGER NOT NULL DEFAULT 0, reject INTEGER NOT NULL DEFAULT 0,
  defer INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0,
  renames INTEGER NOT NULL DEFAULT 0,
  queue_causes INTEGER, queue_effects INTEGER, elapsed_min INTEGER,
  detail_json TEXT NOT NULL, created_at TEXT NOT NULL
);
`);

// ━━━ 迁移：老库缺列自动补（云端 8-21 dump 已含全部列，此段保新老库兼容）━━━
(function migrate() {
  const colsOf = (t) => new Set(DB.prepare(`PRAGMA table_info(${t})`).all().map((r) => r.name));
  const evCols = colsOf('events');
  if (!evCols.has('vector_dirty')) DB.exec('ALTER TABLE events ADD COLUMN vector_dirty INTEGER DEFAULT 1');
  const ccCols = colsOf('event_communities');
  if (!ccCols.has('edge_type')) DB.exec(`ALTER TABLE event_communities ADD COLUMN edge_type TEXT NOT NULL DEFAULT 'all'`);
  if (!ccCols.has('created_at')) DB.exec('ALTER TABLE event_communities ADD COLUMN created_at TEXT');
  if (!ccCols.has('updated_at')) DB.exec('ALTER TABLE event_communities ADD COLUMN updated_at TEXT');
  // 老版 causal_reviews（conv_uuid/status/detail）→ 改名留档，建新版
  const crCols = colsOf('causal_reviews');
  if (crCols.has('status') && !crCols.has('verdict')) {
    DB.exec('ALTER TABLE causal_reviews RENAME TO causal_reviews_legacy');
    DB.exec(`CREATE TABLE IF NOT EXISTS causal_reviews (
      id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL, candidate_id INTEGER,
      verdict TEXT NOT NULL, reason TEXT, decided_by TEXT, decided_at TEXT NOT NULL, defer_until TEXT
    )`);
    console.log('migrate: causal_reviews 老结构已归档为 causal_reviews_legacy');
  }
  // profile 加序号列(2026-09-15): id=1..n 连续, 老卡按建档顺序(rowid)编 —— 排序/拼接都按它
  const pfCols = colsOf('profile');
  if (pfCols.size > 0 && !pfCols.has('id')) {
    DB.exec('ALTER TABLE profile ADD COLUMN id INTEGER');
    const pfRows = DB.prepare('SELECT title FROM profile ORDER BY rowid ASC').all();
    DB.exec('BEGIN');
    const pfUpd = DB.prepare('UPDATE profile SET id = ? WHERE title = ?');
    pfRows.forEach((r, i) => pfUpd.run(i + 1, r.title));
    DB.exec('COMMIT');
    console.log(`migrate: profile.id 已补齐(1..${pfRows.length} 建档顺序)`);
  }
})();

// ━━━ 向量表（sqlite-vec）━━━
let vecReady = false;
try {
  const { load } = require('sqlite-vec');
  load(DB);
  DB.exec(`CREATE VIRTUAL TABLE IF NOT EXISTS vec_events USING vec0(embedding float[1024]);`);
  DB.exec(`CREATE TABLE IF NOT EXISTS vec_events_meta (rowid INTEGER PRIMARY KEY, event_id INTEGER NOT NULL, kind TEXT NOT NULL, frag_idx INTEGER, frag_text TEXT);`);
  vecReady = true;
  console.log('sqlite-vec ready');
} catch (e) {
  console.log('sqlite-vec unavailable:', e.message, '—— recall 降级为字面搜索');
}

// ━━━ 小工具 ━━━
function ts() { return new Date().toISOString(); }
function tsMs(s) { if (!s) return null; const t = Date.parse(s); return Number.isNaN(t) ? null : t; }
function clampInt(value, fallback, min, max) {
  if (value === null || value === undefined || value === '') return fallback;
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

// ━━━ 向量表读写（BigInt rowid + Float32 小端 blob；读回 vec_to_json 优先，blob 解码兜底）━━━
function vecBind(vec) { return new Uint8Array(new Float32Array(vec).buffer); }
function vecPut(table, rowid, vec) {
  // vec0 虚拟表对已有 rowid 不支持 INSERT OR REPLACE（UNIQUE constraint），改为先删后插；
  // 调用方在事务里就并入其事务，否则自己开事务保证原子（2026-08-31 修，见 REST /memories 重发路径）
  const inTx = DB.isTransaction;
  if (!inTx) DB.exec('BEGIN');
  try {
    DB.prepare(`DELETE FROM ${table} WHERE rowid = ?`).run(BigInt(rowid));
    DB.prepare(`INSERT INTO ${table} (rowid, embedding) VALUES (?, ?)`).run(BigInt(rowid), vecBind(vec));
    if (!inTx) DB.exec('COMMIT');
  } catch (e) {
    if (!inTx) { try { DB.exec('ROLLBACK'); } catch {} }
    throw e;
  }
}
function readVecRow(table, rowid) {
  try {
    const r = DB.prepare(`SELECT vec_to_json(embedding) AS j FROM ${table} WHERE rowid = ?`).get(BigInt(rowid));
    if (r && r.j) return JSON.parse(r.j);
  } catch { /* vec_to_json 不可用 → 走 blob 解码 */ }
  const r2 = DB.prepare(`SELECT embedding AS b FROM ${table} WHERE rowid = ?`).get(BigInt(rowid));
  if (r2 && r2.b) {
    const u8 = r2.b instanceof Uint8Array ? r2.b : new Uint8Array(r2.b);
    return Array.from(new Float32Array(u8.buffer, u8.byteOffset, u8.byteLength / 4));
  }
  return null;
}
function vecDelRange(table, lo, hi) {
  DB.prepare(`DELETE FROM ${table} WHERE rowid BETWEEN ? AND ?`).run(BigInt(lo), BigInt(hi));
}

// ━━━ 原文留底（承接云端 R2：archive/{uuid}.json → data/raw/{uuid}.json）━━━

// ━━━ parser: raw json → blocks（与云端 parseRaw 一致）━━━

// ━━━ D1 ops（conversations / blocks / chunks）━━━
function dbStats() {
  const ev = DB.prepare('SELECT COUNT(*) AS c FROM events').get();
  const edges = DB.prepare('SELECT COUNT(*) AS c FROM event_edges').get();
  const comms = DB.prepare('SELECT COUNT(*) AS c FROM event_communities').get();
  const oldest = DB.prepare('SELECT MIN(time_span_start) AS t FROM events').get();
  const newest = DB.prepare('SELECT MAX(time_span_end) AS t FROM events').get();
  return { event_count: ev?.c || 0, edge_count: edges?.c || 0, community_count: comms?.c || 0, oldest: oldest?.t, newest: newest?.t };
}



// ━━━ 小工具 ━━━
const _axisCache = new Map();


// ━━━ profile 档案卡（助手的自我叙事/关系认知 → 下一轮 system prompt；MCP 与 REST 共用）━━━
// id = 序号(始终 1..n 连续), 拼接/排序都按它; title 仍是唯一键。移动/改名后其余卡自动重排。
function dbListProfile() {
  return DB.prepare('SELECT id, title, content, updated_at FROM profile ORDER BY id ASC').all();
}
function dbAssembleProfile() {
  return dbListProfile().map((it) => `#${it.id} ${it.title}\n\n${it.content}`).join('\n\n');
}
function dbGetProfile(target) {
  // target: {id} 或 {title}, id 优先
  if (target.id != null) return DB.prepare('SELECT id, title, content, updated_at FROM profile WHERE id = ?').get(target.id) ?? null;
  if (target.title != null) return DB.prepare('SELECT id, title, content, updated_at FROM profile WHERE title = ?').get(target.title) ?? null;
  return null;
}
function dbUpsertProfile(title, content) {
  const existing = dbGetProfile({ title });
  const now = ts();
  if (existing) {
    DB.prepare('UPDATE profile SET content = ?, updated_at = ? WHERE title = ?').run(content, now, title);
    return { created: false, updated_at: now, card: dbGetProfile({ title }) };
  }
  const next = (DB.prepare('SELECT MAX(id) AS m FROM profile').get()?.m ?? 0) + 1;
  DB.prepare('INSERT INTO profile (id, title, content, updated_at) VALUES (?, ?, ?, ?)').run(next, title, content, now);
  return { created: true, updated_at: now, card: dbGetProfile({ title }) };
}
function dbRewriteProfileOrder(orderedTitles) {
  DB.exec('BEGIN');
  try {
    const upd = DB.prepare('UPDATE profile SET id = ? WHERE title = ?');
    orderedTitles.forEach((t, i) => upd.run(i + 1, t));
    DB.exec('COMMIT');
  } catch (e) { DB.exec('ROLLBACK'); throw e; }
}
function dbRenameProfile(target, newTitle) {
  const card = dbGetProfile(target);
  if (!card) return { error: 'profile card not found', status: 404 };
  if (newTitle === card.title) return { card };
  if (dbGetProfile({ title: newTitle })) return { error: `同名卡已存在: ${newTitle}`, status: 409 };
  DB.prepare('UPDATE profile SET title = ?, updated_at = ? WHERE title = ?').run(newTitle, ts(), card.title);
  return { card: dbGetProfile({ title: newTitle }) };
}
function dbMoveProfile(target, newId) {
  const rows = dbListProfile();
  const idx = rows.findIndex((r) => (target.id != null ? r.id === Number(target.id) : r.title === target.title));
  if (idx < 0) return { error: 'profile card not found', status: 404 };
  const card = rows[idx];
  const pos = Math.max(1, Math.min(Number(newId) || 1, rows.length)) - 1;
  rows.splice(idx, 1);
  rows.splice(pos, 0, card);
  dbRewriteProfileOrder(rows.map((r) => r.title));
  return { card: dbGetProfile({ title: card.title }) };
}

// ━━━ night（助手的夜间整理：唤醒开关 + 晨报存档；MCP 与 REST 共用）━━━
function dbNightSwitch() {
  const row = DB.prepare('SELECT enabled, updated_at FROM night_switch WHERE id = 1').get();
  if (row) return { enabled: row.enabled === 1, updated_at: row.updated_at };
  DB.prepare('INSERT INTO night_switch (id, enabled, updated_at) VALUES (1, 1, ?)').run(ts());
  return { enabled: true, updated_at: ts() };
}
function dbNightSetEnabled(enabled) {
  DB.prepare(`INSERT INTO night_switch (id, enabled, updated_at) VALUES (1, ?, ?)
    ON CONFLICT(id) DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at`)
    .run(enabled ? 1 : 0, ts());
  return dbNightSwitch();
}
function dbNightReportSummary(row) {
  return {
    date: row.date, verdicts_total: row.verdicts_total,
    link: row.link, reject: row.reject, defer: row.defer, done: row.done,
    renames: row.renames,
    queue_left: { causes: row.queue_causes, effects: row.queue_effects },
    elapsed_min: row.elapsed_min, created_at: row.created_at,
  };
}
// night.js 收尾上报，按日期整份覆盖（同晚重跑 = 最新一份）
function dbNightUpsertReport(r) {
  const verdicts = Array.isArray(r.verdicts) ? r.verdicts : [];
  const renames = Array.isArray(r.renames) ? r.renames : [];
  const cnt = { link: 0, reject: 0, defer: 0, done: 0 };
  for (const v of verdicts) if (cnt[v.verdict] != null) cnt[v.verdict]++;
  const detail = {
    dream_words: typeof r.dream_words === 'string' ? r.dream_words : null,
    verdicts, renames, notes: Array.isArray(r.notes) ? r.notes : [],
    timeline: Array.isArray(r.timeline) ? r.timeline : [],
    tokens: r.tokens && typeof r.tokens === 'object' ? r.tokens : null,
  };
  DB.prepare(`INSERT INTO night_reports (date, verdicts_total, link, reject, defer, done, renames, queue_causes, queue_effects, elapsed_min, detail_json, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(date) DO UPDATE SET verdicts_total=excluded.verdicts_total, link=excluded.link, reject=excluded.reject,
      defer=excluded.defer, done=excluded.done, renames=excluded.renames, queue_causes=excluded.queue_causes,
      queue_effects=excluded.queue_effects, elapsed_min=excluded.elapsed_min, detail_json=excluded.detail_json, created_at=excluded.created_at`)
    .run(String(r.date), verdicts.length, cnt.link, cnt.reject, cnt.defer, cnt.done, renames.length,
      r.queue_left?.causes ?? null, r.queue_left?.effects ?? null, r.elapsed_min ?? null,
      JSON.stringify(detail), ts());
  return DB.prepare('SELECT * FROM night_reports WHERE date = ?').get(r.date);
}

// ━━━ events ops ━━━
function dbCreateEvent(title, summary, time_span_start, time_span_end, themes, source_block_ids, importance, event_type) {
  const now = ts();
  const themesJson = themes && themes.length > 0 ? JSON.stringify(themes) : null;
  const blockIdsJson = source_block_ids && source_block_ids.length > 0 ? JSON.stringify(source_block_ids) : null;
  const r = DB.prepare(`INSERT INTO events (title, summary, time_span_start, time_span_end, themes, source_block_ids, importance, event_type, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(title, summary, time_span_start, time_span_end, themesJson, blockIdsJson, importance, event_type, now, now);
  return {
    id: Number(r.lastInsertRowid), title, summary, time_span_start, time_span_end,
    themes: themesJson, source_block_ids: blockIdsJson, importance, event_type,
    created_at: now, updated_at: now,
  };
}
function dbGetEvent(id) {
  return DB.prepare(`SELECT id, title, summary, time_span_start, time_span_end, themes, source_block_ids, importance, event_type, created_at, updated_at
                     FROM events WHERE id = ?`).get(id) ?? null;
}
function dbListEvents(params) {
  const conditions = []; const binds = [];
  if (params.q) { conditions.push('(title LIKE ? OR themes LIKE ?)'); binds.push(`%${params.q}%`, `%${params.q}%`); }
  if (params.theme) { conditions.push('themes LIKE ?'); binds.push(`%${params.theme}%`); }
  if (params.event_type) { conditions.push('event_type = ?'); binds.push(params.event_type); }
  if (params.time_start) { conditions.push('time_span_end IS NULL OR time_span_end >= ?'); binds.push(params.time_start); }
  if (params.time_end) { conditions.push('time_span_start <= ?'); binds.push(params.time_end); }
  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  const sortCol = params.sort === 'importance' ? 'importance DESC' : params.sort === 'created' ? 'created_at DESC' : params.sort === 'time_asc' ? 'time_span_start ASC' : 'time_span_start DESC';
  const limit = clampInt(params.limit, 10, -1, 200);
  const offset = clampInt(params.offset, 0, 0, Number.MAX_SAFE_INTEGER);
  const items = DB.prepare(`SELECT id, title, summary, time_span_start, time_span_end, themes, source_block_ids, importance, event_type, created_at, updated_at
                            FROM events ${where} ORDER BY ${sortCol} LIMIT ? OFFSET ?`).all(...binds, limit, offset);
  const cnt = DB.prepare(`SELECT COUNT(*) AS c FROM events ${where}`).get(...binds);
  return { items, total: cnt?.c || 0 };
}
function dbUpdateEvent(id, fields) {
  const sets = []; const binds = [];
  if (fields.title !== undefined) { sets.push('title = ?'); binds.push(fields.title); }
  if (fields.summary !== undefined) { sets.push('summary = ?'); binds.push(fields.summary); }
  if (fields.time_span_start !== undefined) { sets.push('time_span_start = ?'); binds.push(fields.time_span_start); }
  if (fields.time_span_end !== undefined) { sets.push('time_span_end = ?'); binds.push(fields.time_span_end); }
  if (fields.themes !== undefined) { sets.push('themes = ?'); binds.push(fields.themes && fields.themes.length > 0 ? JSON.stringify(fields.themes) : null); }
  if (fields.source_block_ids !== undefined) { sets.push('source_block_ids = ?'); binds.push(fields.source_block_ids && fields.source_block_ids.length > 0 ? JSON.stringify(fields.source_block_ids) : null); }
  if (fields.importance !== undefined) { sets.push('importance = ?'); binds.push(fields.importance); }
  if (fields.event_type !== undefined) { sets.push('event_type = ?'); binds.push(fields.event_type); }
  if (sets.length === 0) return false;
  sets.push('updated_at = ?');
  binds.push(ts());
  binds.push(id);
  const r = DB.prepare(`UPDATE events SET ${sets.join(', ')} WHERE id = ?`).run(...binds);
  return r.changes > 0;
}
function dbDeleteEvent(id) { return DB.prepare('DELETE FROM events WHERE id = ?').run(id).changes > 0; }
function dbMarkEventDirty(id) { DB.prepare('UPDATE events SET vector_dirty = 1 WHERE id = ?').run(id); }
function dbMarkEventClean(id) { DB.prepare('UPDATE events SET vector_dirty = 0 WHERE id = ?').run(id); }
function dbListDirtyEvents() {
  return DB.prepare(`SELECT id, title, summary, time_span_start, time_span_end, themes, source_block_ids, importance, event_type, created_at, updated_at, vector_dirty
                     FROM events WHERE vector_dirty = 1`).all();
}
function dbListAllEvents() {
  return DB.prepare(`SELECT id, title, summary, time_span_start, time_span_end, themes, source_block_ids, importance, event_type, created_at, updated_at, vector_dirty
                     FROM events`).all();
}
function dbEventTitleMap(ids) {
  const titleMap = new Map();
  if (ids.length === 0) return titleMap;
  for (let i = 0; i < ids.length; i += EVENT_TITLE_BATCH) {
    const chunk = ids.slice(i, i + EVENT_TITLE_BATCH);
    const placeholders = chunk.map(() => '?').join(',');
    for (const e of DB.prepare(`SELECT id, title FROM events WHERE id IN (${placeholders})`).all(...chunk)) titleMap.set(e.id, e.title);
  }
  return titleMap;
}

// ━━━ 事件向量：多向量方案（whole + frag），rowid = eventId*1000(+fragIdx+1) ━━━
function eventEmbedText(ev) { return `${ev.title}\n${ev.summary ?? ''}`.trim(); }
function splitEventFragments(summary, maxLen = 100) {
  if (!summary) return [];
  const paras = summary.split(/\n\s*\n/).map((s) => s.trim()).filter(Boolean);
  if (paras.length > 1) return paras.filter((f) => f.length >= 8);   // 多段: 按自然段
  const single = (paras[0] ?? '').trim();
  if (!single) return [];
  if (single.length <= 200) return [];   // 短单段: 不切
  const sentences = single.split(/(?<=[。！？；;])\s*/).map((s) => s.trim()).filter(Boolean);
  const frags = [];
  let cur = '';
  for (const s of sentences) {
    if ((cur + s).length <= maxLen) { cur += s; }
    else { if (cur) frags.push(cur); cur = s; }
  }
  if (cur) frags.push(cur);
  return frags.filter((f) => f.length >= 8);
}
function buildEventVectorSpecs(ev) {
  const specs = [{ id: ev.id * EVENT_ROWID_SCALE, text: eventEmbedText(ev), frag_idx: null, frag_text: null }];
  const frags = splitEventFragments(ev.summary);
  frags.forEach((f, i) => {
    specs.push({ id: ev.id * EVENT_ROWID_SCALE + i + 1, text: f, frag_idx: i, frag_text: f });
  });
  return specs;
}
function matchKindOf(rowid) { return rowid % EVENT_ROWID_SCALE === 0 ? 'whole' : 'frag'; }
function deleteEventVectors(eventId) {
  vecDelRange('vec_events', eventId * EVENT_ROWID_SCALE, eventId * EVENT_ROWID_SCALE + EVENT_ROWID_SCALE - 1);
  DB.prepare('DELETE FROM vec_events_meta WHERE event_id = ?').run(eventId);
}

// ━━━ 事件混合检索（语义 frag 向量 + BM25 标题/正文，RRF 融合，返回完整事件）━━━
function bm25Tokenize(text) {
  const s = String(text ?? '').toLowerCase();
  const tokens = [];
  for (const m of s.matchAll(/[a-z0-9_]+/g)) tokens.push(m[0]);
  // 中文无分词器 → 连续 CJK/假名段做二元组切词（单字段保留单字）
  for (const run of (s.match(/[\u4e00-\u9fff\u3040-\u30ff]+/g) ?? [])) {
    if (run.length === 1) { tokens.push(run); continue; }
    for (let i = 0; i < run.length - 1; i++) tokens.push(run.slice(i, i + 2));
  }
  return tokens;
}
function bm25RankEvents(query) {
  const qTokens = [...new Set(bm25Tokenize(query))];
  if (qTokens.length === 0) return [];
  const events = DB.prepare('SELECT id, title, summary FROM events').all();
  const docs = events.map((ev) => {
    const tokens = bm25Tokenize(`${ev.title}\n${ev.title}\n${ev.summary ?? ''}`);   // 标题算两遍=轻量字段加权
    const tf = new Map();
    for (const t of tokens) tf.set(t, (tf.get(t) ?? 0) + 1);
    return { id: ev.id, tf, len: tokens.length };
  });
  const N = docs.length;
  const avgLen = docs.reduce((s, d) => s + d.len, 0) / (N || 1);
  const scored = [];
  for (const d of docs) {
    let score = 0;
    for (const qt of qTokens) {
      const f = d.tf.get(qt);
      if (!f) continue;
      let df = 0;
      for (const x of docs) if (x.tf.has(qt)) df++;
      const idf = Math.log((N - df + 0.5) / (df + 0.5) + 1);
      score += idf * (f * (BM25_K1 + 1)) / (f + BM25_K1 * (1 - BM25_B + BM25_B * d.len / avgLen));
    }
    if (score > 0) scored.push({ event_id: d.id, score: Math.round(score * 10000) / 10000 });
  }
  scored.sort((a, b) => b.score - a.score);
  return scored;
}
async function archiveSearchEvents(params) {
  const query = String(params.query ?? '').trim();
  if (!query) throw new Error('query is empty');
  const k = Math.min(Math.max(Number(params.k ?? 5), 1), 20);

  // 语义路: query → vec_events KNN → 只认 frag 命中(命中切块=命中所属事件); 无 frag 的事件用 whole 兜底
  const semByEvent = new Map();
  const wholeHits = new Map();
  let semanticDegraded = false;
  try {
    const truncated = query.length > EMBED_QUERY_MAX ? query.slice(0, EMBED_QUERY_MAX) : query;
    const vecs = await embedTexts([truncated]);
    if (vecs.length === 0) throw new Error('embedding returned no vectors');
    const overFetchK = Math.min(400, Math.max(k * 10, 60));
    const rows = DB.prepare(`SELECT rowid, distance FROM vec_events WHERE embedding MATCH ? AND k = ?`).all(vecBind(vecs[0]), overFetchK);
    for (const row of rows) {
      const meta = DB.prepare('SELECT event_id, kind, frag_text FROM vec_events_meta WHERE rowid = ?').get(Number(row.rowid));
      if (!meta) continue;
      const score = Math.round((1 - row.distance) * 10000) / 10000;
      if (meta.kind === 'frag') {
        const cur = semByEvent.get(meta.event_id);
        if (!cur || score > cur.score) semByEvent.set(meta.event_id, { score, kind: 'frag', frag_text: meta.frag_text });
      } else {
        wholeHits.set(meta.event_id, score);
      }
    }
    for (const [eventId, score] of wholeHits) {
      if (!semByEvent.has(eventId)) semByEvent.set(eventId, { score, kind: 'whole', frag_text: null });
    }
  } catch (e) {
    semanticDegraded = true;   // embed-server 不可用时不挡关键词路
  }
  const semRanked = [...semByEvent.entries()].map(([eventId, hit]) => ({ event_id: eventId, ...hit })).sort((a, b) => b.score - a.score);

  // 关键词路: BM25 over 标题+正文(不含 themes/时间等元数据)
  const bm25Ranked = bm25RankEvents(query);

  // RRF 融合: score = Σ 1/(60+rank)，两路都命中的自然浮上来
  const RRF_K = 60;
  const fused = new Map();
  const addRank = (list, key) => list.forEach((r, i) => {
    const cur = fused.get(r.event_id) ?? { rrf: 0, sem_hit: null, bm25_score: null };
    cur.rrf += 1 / (RRF_K + i + 1);
    if (key === 'sem') cur.sem_hit = r;
    else cur.bm25_score = r.score;
    fused.set(r.event_id, cur);
  });
  addRank(semRanked, 'sem');
  addRank(bm25Ranked, 'bm25');
  const top = [...fused.entries()]
    .sort((a, b) => b[1].rrf - a[1].rrf || (b[1].sem_hit?.score ?? 0) - (a[1].sem_hit?.score ?? 0))
    .slice(0, k);

  const matches = top.map(([eventId, f]) => {
    const ev = dbGetEvent(eventId);
    if (!ev) return null;
    return {
      event: ev,
      rrf: Math.round(f.rrf * 10000) / 10000,
      semantic: f.sem_hit ? { score: f.sem_hit.score, kind: f.sem_hit.kind, frag_text: f.sem_hit.frag_text } : null,
      bm25_score: f.bm25_score,
    };
  }).filter(Boolean);

  return {
    ok: true, query, k,
    semantic_degraded: semanticDegraded,
    semantic_candidates: semRanked.length,
    bm25_candidates: bm25Ranked.length,
    matches,
  };
}

// ━━━ 社区聚类（Louvain，云端 runCommunityDetection 移植）━━━
function runCommunityDetection() {
  const edges = DB.prepare('SELECT event_a, event_b FROM event_edges').all();
  const events = DB.prepare('SELECT id, time_span_start, time_span_end FROM events').all();
  if (events.length === 0) {
    DB.prepare('DELETE FROM event_communities').run();
    return { communities: 0, preserved: 0 };
  }
  const partition = louvain(events.map((e) => e.id), edges.map((e) => [e.event_a, e.event_b]));
  const groups = new Map();
  for (const [nid, cid] of partition) {
    if (!groups.has(cid)) groups.set(cid, []);
    groups.get(cid).push(nid);
  }
  const eventById = new Map(events.map((e) => [e.id, e]));
  const edgeSet = new Map();
  for (const e of edges) {
    const key = `${Math.min(e.event_a, e.event_b)}:${Math.max(e.event_a, e.event_b)}`;
    edgeSet.set(key, (edgeSet.get(key) ?? 0) + 1);
  }
  // 旧命名社区(renamed=1): Jaccard 重叠率 ≥ 0.8 继承名字
  const oldRenamed = DB.prepare('SELECT * FROM event_communities WHERE renamed = 1').all();
  const oldNamed = oldRenamed.map((r) => ({ name: r.name, note: r.note, members: new Set(JSON.parse(r.member_ids)), used: false }));
  const NAME_KEEP_RATIO = 0.8;
  DB.prepare('DELETE FROM event_communities').run();
  const now = ts();
  const sortedGroups = [...groups.entries()].sort((a, b) => b[1].length - a[1].length);
  const defaultIdx = { n: 0 };
  let preserved = 0;
  for (const [, members] of sortedGroups) {
    const memberSet = new Set(members);
    let internal = 0;
    for (const [key, w] of edgeSet) {
      const [a, b] = key.split(':').map(Number);
      if (memberSet.has(a) && memberSet.has(b)) internal += w;
    }
    const degreeIn = new Map();
    for (const [key, w] of edgeSet) {
      const [a, b] = key.split(':').map(Number);
      if (memberSet.has(a) && memberSet.has(b)) {
        degreeIn.set(a, (degreeIn.get(a) ?? 0) + w);
        degreeIn.set(b, (degreeIn.get(b) ?? 0) + w);
      }
    }
    let core = members[0];
    let bestDeg = -1;
    for (const m of members) {
      const d = degreeIn.get(m) ?? 0;
      if (d > bestDeg) { bestDeg = d; core = m; }
    }
    const spans = members.map((m) => eventById.get(m)?.time_span_start ?? null).filter((s) => !!s).sort();
    const spanStart = spans[0] ?? null;
    const spanEnd = spans[spans.length - 1] ?? null;
    // 命名: Jaccard 重叠率最高且 ≥ 0.8 的旧命名社区继承名字(贪心)
    const curMemberSet = new Set(members);
    let oldMatch = null;
    let bestRatio = NAME_KEEP_RATIO;
    let bestOldIdx = -1;
    oldNamed.forEach((oldC, i) => {
      if (oldC.used) return;
      let inter = 0;
      for (const m of members) if (oldC.members.has(m)) inter++;
      const union = curMemberSet.size + oldC.members.size - inter;
      const ratio = union > 0 ? inter / union : 0;
      if (ratio >= bestRatio) { bestRatio = ratio; bestOldIdx = i; }
    });
    if (bestOldIdx >= 0) {
      oldMatch = { name: oldNamed[bestOldIdx].name, note: oldNamed[bestOldIdx].note };
      oldNamed[bestOldIdx].used = true;
      preserved++;
    }
    let name = oldMatch?.name ?? '';
    const note = oldMatch?.note ?? null;
    const renamed = oldMatch ? 1 : 0;
    if (!name) { defaultIdx.n++; name = `未命名·社区${defaultIdx.n}`; }
    DB.prepare(`INSERT INTO event_communities (name, note, member_ids, core_event_id, internal_edges, time_span_start, time_span_end, edge_type, renamed, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'all', ?, ?, ?)`)
      .run(name, note, JSON.stringify(members), core, internal, spanStart, spanEnd, renamed, now, now);
  }
  return { communities: sortedGroups.length, preserved };
}

// ━━━ 网络重建（云端 rebuildEventNetwork 移植，本地 qwen）━━━
let _rebuilding = false;   // 防并发重建

function rebuildLog(line) {
  const l = `[${ts()}] ${line}`;
  console.log(l);
  try { fs.appendFileSync(REBUILD_LOG, l + '\n', 'utf8'); } catch {}
}

async function rebuildEventNetwork(onProgress) {
  if (_rebuilding) throw new Error('rebuild already running');
  _rebuilding = true;
  const emit = (e) => { try { onProgress?.(e); } catch {} };
  try {
    // 崩溃安全: 清空前整表备份到 event_edges_backup; 成功后删备份, 失败保留(可 restore-network)
    DB.exec('DROP TABLE IF EXISTS event_edges_backup');
    DB.exec(`CREATE TABLE event_edges_backup AS SELECT event_a, event_b, edge_type, detail, confidence, created_at FROM event_edges`);
    const dirty = dbListDirtyEvents();
    const all = dbListAllEvents();
    rebuildLog(`rebuild 开始: dirty=${dirty.length} all=${all.length} threshold=${THEMATIC_THRESHOLD}`);

    // 1. 增量 embed: 脏事件 → whole+frag 向量 → vec_events
    let embedded = 0;
    const embedFailures = [];
    const batchTexts = []; const batchSpecs = []; const eventEnds = [];
    for (const ev of dirty) {
      const specs = buildEventVectorSpecs(ev);
      for (const s of specs) { batchTexts.push(s.text); batchSpecs.push(s); }
      eventEnds.push(batchTexts.length);
    }
    let embedDone = 0;
    let embedFailedNow = 0;
    const emitEmbed = () => {
      const cur = dirty[embedDone];
      emit({ phase: 'embed', dirty_total: dirty.length, done: embedDone, failed: embedFailedNow, ...(cur ? { title: cur.title } : {}) });
    };
    emitEmbed();
    if (batchTexts.length > 0) {
      for (let i = 0; i < batchTexts.length; i += EMBED_BATCH) {
        const texts = batchTexts.slice(i, i + EMBED_BATCH);
        const specs = batchSpecs.slice(i, i + EMBED_BATCH);
        try {
          const vecs = await embedTexts(texts);
          const insMeta = DB.prepare('INSERT OR REPLACE INTO vec_events_meta (rowid, event_id, kind, frag_idx, frag_text) VALUES (?, ?, ?, ?, ?)');
          DB.exec('BEGIN');
          for (let j = 0; j < specs.length; j++) {
            const s = specs[j];
            const isWhole = s.frag_idx === null;
            vecPut('vec_events', s.id, vecs[j]);
            insMeta.run(BigInt(s.id), Math.floor(s.id / EVENT_ROWID_SCALE), isWhole ? 'whole' : 'frag',
              isWhole ? null : s.frag_idx, s.frag_text ?? null);
          }
          DB.exec('COMMIT');
          embedded += specs.length;
          while (embedDone < eventEnds.length && eventEnds[embedDone] <= i + texts.length) embedDone++;
          emitEmbed();
          if (i % (EMBED_BATCH * 5) === 0) rebuildLog(`embed 进度: ${i}/${batchTexts.length} 条向量`);
        } catch (e) {
          // 整批失败 → 逐条重试隔离
          rebuildLog(`embed 批量失败 @${i}: ${e.message} → 逐条重试`);
          for (let j = 0; j < specs.length; j++) {
            const s = specs[j];
            try {
              const vecs = await embedTexts([s.text]);
              const isWhole = s.id % EVENT_ROWID_SCALE === 0;
              vecPut('vec_events', s.id, vecs[0]);
              DB.prepare('INSERT OR REPLACE INTO vec_events_meta (rowid, event_id, kind, frag_idx, frag_text) VALUES (?, ?, ?, ?, ?)')
                .run(BigInt(s.id), Math.floor(s.id / EVENT_ROWID_SCALE), isWhole ? 'whole' : 'frag',
                  isWhole ? null : (s.id % EVENT_ROWID_SCALE) - 1, s.frag_text ?? null);
              embedded++;
            } catch (e2) {
              const evId = Math.floor(s.id / EVENT_ROWID_SCALE);
              if (!embedFailures.includes(evId)) { embedFailures.push(evId); embedFailedNow++; }
              rebuildLog(`embed 单条失败 event=${evId}: ${e2.message}`);
            }
          }
          while (embedDone < eventEnds.length && eventEnds[embedDone] <= i + texts.length) embedDone++;
          emitEmbed();
        }
      }
    }
    // 标干净(成功的事件)
    for (const ev of dirty) if (!embedFailures.includes(ev.id)) dbMarkEventClean(ev.id);

    // 2. 全量重算边(备份手动边)
    const manualEdges = DB.prepare(`SELECT event_a, event_b, edge_type, detail, confidence FROM event_edges WHERE detail LIKE '%manual%'`).all();
    DB.prepare('DELETE FROM event_edges').run();
    const insEdge = DB.prepare(`INSERT OR IGNORE INTO event_edges (event_a, event_b, edge_type, detail, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?)`);
    for (const me of manualEdges) insEdge.run(me.event_a, me.event_b, me.edge_type, me.detail, me.confidence, ts());

    let overlap = 0;
    const seen = new Set();
    // 2a. 时间重叠
    emit({ phase: 'edges', step: 'overlap' });
    for (let i = 0; i < all.length; i++) {
      const a = all[i];
      for (let j = i + 1; j < all.length; j++) {
        const b = all[j];
        const aEnd = a.time_span_end ?? a.time_span_start;
        const bEnd = b.time_span_end ?? b.time_span_start;
        if (!a.time_span_start || !b.time_span_start) continue;
        if (a.time_span_start <= bEnd && b.time_span_start <= aEnd) {
          const key = `${Math.min(a.id, b.id)}:${Math.max(a.id, b.id)}:overlap`;
          if (seen.has(key)) continue;
          seen.add(key);
          insEdge.run(Math.min(a.id, b.id), Math.max(a.id, b.id), 'overlap', null, 1.0, ts());
          overlap++;
        }
      }
    }
    rebuildLog(`edges: overlap=${overlap}`);

    // 2b. 语义邻居: 内存暴力余弦(qwen 归一化向量, dot=cos)。向量直接从 vec_events 读回
    emit({ phase: 'edges', step: 'thematic' });
    const failedIds = new Set(embedFailures);
    const specsAll = [];   // {rowid, event_id, kind, frag_text, vec}
    for (const ev of all) {
      if (failedIds.has(ev.id)) continue;
      const specs = buildEventVectorSpecs(ev);
      for (const s of specs) {
        const vec = readVecRow('vec_events', s.id);
        if (vec) specsAll.push({ rowid: s.id, event_id: ev.id, kind: s.frag_idx === null ? 'whole' : 'frag', frag_text: s.frag_text, vec });
      }
    }
    const bestMatch = new Map();   // key = a:b → {score, kind, fragText}
    for (let i = 0; i < specsAll.length; i++) {
      const si = specsAll[i];
      for (let j = i + 1; j < specsAll.length; j++) {
        const sj = specsAll[j];
        if (si.event_id === sj.event_id) continue;
        let dot = 0;
        for (let d = 0; d < si.vec.length; d++) dot += si.vec[d] * sj.vec[d];
        const key = `${Math.min(si.event_id, sj.event_id)}:${Math.max(si.event_id, sj.event_id)}`;
        const prev = bestMatch.get(key);
        if (!prev || dot > prev.score) bestMatch.set(key, { score: dot, kind: (si.kind === 'whole' && sj.kind === 'whole') ? 'whole' : 'frag', fragText: si.frag_text ?? sj.frag_text });
      }
    }
    // 分布统计（校准阈值用, 写日志）
    const scores = [...bestMatch.values()].map((m) => m.score).sort((a, b) => a - b);
    const pct = (p) => scores.length > 0 ? scores[Math.min(scores.length - 1, Math.floor(scores.length * p))] : 0;
    rebuildLog(`thematic 分数分布: pairs=${scores.length} p50=${pct(0.5).toFixed(4)} p75=${pct(0.75).toFixed(4)} p90=${pct(0.9).toFixed(4)} p95=${pct(0.95).toFixed(4)} p99=${pct(0.99).toFixed(4)} max=${(scores[scores.length - 1] ?? 0).toFixed(4)}`);
    let thematic = 0;
    const aliveIds = new Set(all.map((e) => e.id));
    for (const [key, m] of bestMatch) {
      if (m.score < THEMATIC_THRESHOLD) continue;
      const [a, b] = key.split(':').map(Number);
      if (!aliveIds.has(a) || !aliveIds.has(b)) continue;
      const edgeKey = `${key}:thematic`;
      if (seen.has(edgeKey)) continue;
      seen.add(edgeKey);
      insEdge.run(a, b, 'thematic', JSON.stringify({
        semantic_score: Math.round(m.score * 10000) / 10000,
        match_kind: m.kind,
        ...(m.fragText ? { frag_text: m.fragText.slice(0, 120) } : {}),
      }), 0.7, ts());
      thematic++;
    }
    rebuildLog(`edges: thematic=${thematic} (threshold ${THEMATIC_THRESHOLD})`);

    // 成功 → 删备份表
    DB.exec('DROP TABLE IF EXISTS event_edges_backup');

    // 3. 社区聚类(Louvain)
    emit({ phase: 'communities' });
    let communities = 0;
    let communities_preserved = 0;
    try {
      const comm = runCommunityDetection();
      communities = comm.communities;
      communities_preserved = comm.preserved;
    } catch (e) {
      rebuildLog(`community detection failed: ${e.message}`);
    }

    const result = {
      embedded,
      embed_failures: embedFailures,
      edges: { overlap, thematic, total: overlap + thematic + manualEdges.length },
      vector_count: all.reduce((acc, ev) => acc + buildEventVectorSpecs(ev).length, 0),
      backup_created: true,
      backup_dropped: true,
      communities,
      communities_preserved,
    };
    rebuildLog(`rebuild 完成: embedded=${embedded} overlap=${overlap} thematic=${thematic} communities=${communities} preserved=${communities_preserved} failures=${embedFailures.length}`);
    return result;
  } finally {
    _rebuilding = false;
  }
}

function restoreEventNetwork() {
  const backup = DB.prepare(`SELECT COUNT(*) AS c FROM sqlite_master WHERE type='table' AND name='event_edges_backup'`).get();
  if (!backup || backup.c === 0) return null;
  DB.prepare('DELETE FROM event_edges').run();
  DB.prepare(`INSERT INTO event_edges (event_a, event_b, edge_type, detail, confidence, created_at)
              SELECT event_a, event_b, edge_type, detail, confidence, created_at FROM event_edges_backup`).run();
  const cnt = DB.prepare('SELECT COUNT(*) AS c FROM event_edges').get();
  DB.exec('DROP TABLE IF EXISTS event_edges_backup');
  return { restored: cnt?.c || 0 };
}

// 两个事件的时间距离(天), 取近点。重叠 → 0; 单点事件 end 视为 start; 缺时间 → -1.
function timeGapDays(aStart, aEnd, bStart, bEnd) {
  if (!aStart || !bStart) return -1;
  const a1 = Date.parse(aStart), a2 = aEnd ? Date.parse(aEnd) : a1;
  const b1 = Date.parse(bStart), b2 = bEnd ? Date.parse(bEnd) : b1;
  if (Number.isNaN(a1) || Number.isNaN(a2) || Number.isNaN(b1) || Number.isNaN(b2)) return -1;
  const gapMs = Math.max(0, Math.max(a1 - b2, b1 - a2));
  return Math.round(gapMs / 86400000);
}

function dbEventNetworkStats() {
  const edges = DB.prepare(`SELECT edge_type, COUNT(*) AS c FROM event_edges GROUP BY edge_type`).all();
  let overlap = 0, thematic = 0;
  for (const e of edges) {
    if (e.edge_type === 'overlap') overlap = e.c;
    else if (e.edge_type === 'thematic') thematic = e.c;
  }
  const perEvent = DB.prepare(`SELECT e.id, e.title, e.event_type, e.time_span_start,
        COALESCE(SUM(CASE WHEN ee.edge_type = 'thematic' THEN 1 ELSE 0 END), 0) AS thematic_n,
        COALESCE(SUM(CASE WHEN ee.edge_type = 'overlap' THEN 1 ELSE 0 END), 0) AS overlap_n
     FROM events e
     LEFT JOIN event_edges ee ON ee.event_a = e.id OR ee.event_b = e.id
     GROUP BY e.id
     ORDER BY (thematic_n + overlap_n) DESC, e.id ASC`).all();
  const structure = dbNetworkStructure();
  return {
    total_events: perEvent.length,
    total_edges: overlap + thematic,
    overlap,
    thematic,
    per_event: perEvent.map((r) => ({ id: r.id, title: r.title, event_type: r.event_type, time_span_start: r.time_span_start, thematic_n: r.thematic_n, overlap_n: r.overlap_n })),
    bridges: structure.bridges,
    cut_vertices: structure.cut_vertices,
  };
}

// ━━━ 全局结构信号：社区映射 / 跨群桥边 / 割点（2026-09-14 本地新增）━━━
function dbEventCommunityMap() {
  const commOf = new Map();
  const commName = new Map();
  try {
    for (const c of DB.prepare('SELECT id, name, member_ids FROM event_communities').all()) {
      commName.set(c.id, c.name || `未命名·社区${c.id}`);
      for (const m of JSON.parse(c.member_ids)) if (!commOf.has(m)) commOf.set(m, c.id);
    }
  } catch { /* 社区表为空或损坏: 按无社区处理 */ }
  return { commOf, commName };
}

function dbNetworkStructure() {
  const edges = DB.prepare('SELECT event_a, event_b, edge_type, detail FROM event_edges').all();
  const evs = DB.prepare('SELECT id, title FROM events').all();
  const { commOf, commName } = dbEventCommunityMap();
  const titleOf = new Map(evs.map((e) => [e.id, e.title]));
  const commLabel = (id) => { const c = commOf.get(id); return c == null ? null : (commName.get(c) ?? `社区${c}`); };

  // 桥边: 两端点属不同故事群的边——群与群的连接组织
  const bridges = [];
  for (const r of edges) {
    const ca = commLabel(r.event_a);
    const cb = commLabel(r.event_b);
    if (ca === cb) continue;   // 同群(含双方都无群)不算桥
    let reason;
    try { const d = r.detail ? JSON.parse(r.detail) : null; if (d && typeof d.reason === 'string' && d.reason) reason = d.reason; } catch { /* detail 非 JSON */ }
    bridges.push({
      event_a: r.event_a, title_a: titleOf.get(r.event_a) ?? `#${r.event_a}`, comm_a: ca,
      event_b: r.event_b, title_b: titleOf.get(r.event_b) ?? `#${r.event_b}`, comm_b: cb,
      edge_type: r.edge_type, ...(reason ? { reason } : {}),
    });
  }
  bridges.sort((x, y) => (x.edge_type === 'causal' ? 0 : 1) - (y.edge_type === 'causal' ? 0 : 1));

  // 割点: 无向图 Tarjan——拿掉后图(所在连通块)断开的枢纽事件
  const adj = new Map(evs.map((e) => [e.id, []]));
  const deg = new Map();
  for (const r of edges) {
    adj.get(r.event_a)?.push(r.event_b);
    adj.get(r.event_b)?.push(r.event_a);
    deg.set(r.event_a, (deg.get(r.event_a) ?? 0) + 1);
    deg.set(r.event_b, (deg.get(r.event_b) ?? 0) + 1);
  }
  const disc = new Map(); const low = new Map(); const parent = new Map();
  const aps = new Set();
  let timer = 0;
  const dfs = (u) => {
    disc.set(u, timer); low.set(u, timer); timer++;
    let children = 0;
    for (const v of adj.get(u) ?? []) {
      if (!disc.has(v)) {
        parent.set(v, u); children++;
        dfs(v);
        low.set(u, Math.min(low.get(u), low.get(v)));
        if (!parent.has(u) && children > 1) aps.add(u);
        else if (parent.has(u) && low.get(v) >= disc.get(u)) aps.add(u);
      } else if (v !== parent.get(u)) {
        low.set(u, Math.min(low.get(u), disc.get(v)));
      }
    }
  };
  for (const e of evs) if (!disc.has(e.id)) dfs(e.id);
  const cut_vertices = [...aps]
    .map((id) => ({ event_id: id, title: titleOf.get(id) ?? `#${id}`, community: commLabel(id), degree: deg.get(id) ?? 0 }))
    .sort((x, y) => y.degree - x.degree)
    .slice(0, 50);

  return { bridges: bridges.slice(0, 100), cut_vertices };
}

// ━━━ 子图抽取：seed 加权扩展 radius 跳, 一次调用替代逐跳 graph（2026-09-14 本地新增）━━━
function buildSubgraph({ seeds, radius, max_nodes, edge_types, include_summaries }) {
  const evRows = DB.prepare('SELECT id, title, summary, event_type, time_span_start, time_span_end FROM events').all();
  const evById = new Map(evRows.map((e) => [e.id, e]));
  const missing = seeds.filter((s) => !evById.has(s));
  if (missing.length > 0) return { error: `event not found: ${missing.join(', ')}` };
  const types = (Array.isArray(edge_types) && edge_types.length > 0)
    ? edge_types.map(String).filter((t) => ['causal', 'thematic', 'overlap'].includes(t))
    : ['causal', 'thematic', 'overlap'];
  if (types.length === 0) return { error: 'edge_types 过滤后为空(可选 causal/thematic/overlap)' };
  const maxHop = clampInt(radius, 2, 1, 3);
  const cap = Math.max(clampInt(max_nodes, 24, 5, 40), seeds.length);

  const { commOf, commName } = dbEventCommunityMap();
  const commLabel = (id) => { const c = commOf.get(id); return c == null ? null : (commName.get(c) ?? `社区${c}`); };

  // 邻接表 + 全边表(方向/理由/语义分数/frag 证据解析好)
  const adj = new Map();
  const allEdges = [];
  for (const r of DB.prepare('SELECT event_a, event_b, edge_type, detail, confidence FROM event_edges').all()) {
    if (!types.includes(r.edge_type)) continue;
    let d = null;
    try { d = r.detail ? JSON.parse(r.detail) : null; } catch { /* 旧数据 detail 非 JSON */ }
    const e = {
      a: r.event_a, b: r.event_b, type: r.edge_type, weight: EDGE_WEIGHTS[r.edge_type] ?? 1.0, confidence: r.confidence,
      ...(r.edge_type === 'causal' ? { dir: parseEdgeDir(r.detail, r.event_a, r.event_b,
        tsMs(evById.get(r.event_a)?.time_span_start), tsMs(evById.get(r.event_b)?.time_span_start)) } : {}),
      ...(d && typeof d.name === 'string' && d.name ? { name: d.name } : {}),
      ...(d && typeof d.reason === 'string' && d.reason ? { reason: d.reason } : {}),
      ...(d && typeof d.semantic_score === 'number' ? { semantic_score: d.semantic_score } : {}),
      ...(d && typeof d.frag_text === 'string' && d.frag_text ? { frag_text: d.frag_text } : {}),
    };
    allEdges.push(e);
    if (!adj.has(e.a)) adj.set(e.a, []);
    if (!adj.has(e.b)) adj.set(e.b, []);
    adj.get(e.a).push({ to: e.b, w: e.weight });
    adj.get(e.b).push({ to: e.a, w: e.weight });
  }

  // seeds 固定入选, 之后按 strength(路径边权和, Dijkstra 最大化)在 maxHop 跳内扩展, 超出裁弱分支
  const str = new Map(seeds.map((s) => [s, 0]));
  const hops = new Map(seeds.map((s) => [s, 0]));
  const selected = new Set(seeds);
  const frontier = new Set();
  for (const s of seeds) {
    for (const { to, w } of adj.get(s) ?? []) {
      if (selected.has(to)) continue;
      if (!str.has(to) || w > (str.get(to) ?? 0)) { str.set(to, w); hops.set(to, 1); frontier.add(to); }
    }
  }
  while (frontier.size > 0 && selected.size < cap) {
    let best = null, bestS = -Infinity;
    for (const n of frontier) { const s = str.get(n) ?? 0; if (s > bestS) { bestS = s; best = n; } }
    frontier.delete(best);
    selected.add(best);
    if ((hops.get(best) ?? 0) >= maxHop) continue;
    for (const { to, w } of adj.get(best) ?? []) {
      if (selected.has(to)) continue;
      const ns = bestS + w;
      if (!str.has(to) || ns > (str.get(to) ?? 0)) { str.set(to, ns); hops.set(to, (hops.get(best) ?? 0) + 1); frontier.add(to); }
    }
  }

  const nodes = [...selected]
    .map((id) => {
      const ev = evById.get(id);
      return {
        id, title: ev.title, event_type: ev.event_type,
        time_span_start: ev.time_span_start, time_span_end: ev.time_span_end,
        community_id: commOf.get(id) ?? null, community: commLabel(id),
        strength: Math.round((str.get(id) ?? 0) * 10000) / 10000, hops: hops.get(id) ?? 0, seed: seeds.includes(id),
        ...(include_summaries ? { summary: summaryClip(ev.summary, 100) } : {}),
      };
    })
    .sort((x, y) => (y.seed ? 1 : 0) - (x.seed ? 1 : 0) || y.strength - x.strength || x.id - y.id);

  const subEdges = allEdges.filter((e) => selected.has(e.a) && selected.has(e.b))
    .sort((x, y) => y.weight - x.weight || y.confidence - x.confidence);
  const edgeCap = cap * 3;
  const edges = subEdges.slice(0, edgeCap).map((e) => ({
    a: e.a, b: e.b, type: e.type,
    ...(e.dir !== undefined ? { dir: e.dir } : {}),
    ...(e.name ? { name: e.name } : {}),
    ...(e.reason ? { reason: e.reason } : {}),
    ...(e.semantic_score !== undefined ? { semantic_score: e.semantic_score } : {}),
    ...(e.frag_text ? { frag_text: e.frag_text } : {}),
  }));

  return {
    ok: true, seeds, radius: maxHop, max_nodes: cap, edge_types: types,
    node_count: nodes.length, nodes,
    edge_count: edges.length, edges_truncated: subEdges.length > edges.length, edges,
    note: edges.length === 0
      ? '选中范围内没有边(seed 可能是孤立事件)——入网: 确认时间跨度后 rebuild, 或 event_link 手动连'
      : '按边权主结构优先剪枝; 读节点全文 archive_read_event, 沿因果走链 archive_causal_chain',
  };
}

// ━━━ 联想层（2026-09-16）：事件图上 PPR 扩散 + A→B 有界路径枚举 ━━━
// 参照 HippoRAG pattern completion（种子 → Personalized PageRank）与 RRP 路径评分（rethinking）。
// 纯读侧: 只碰 event_edges/events, 不写库不做 embedding 写入。
const PPR_TELEPORT = 0.15;      // 重启概率: 联想始终贴着种子簇
const PPR_MAX_ITERS = 60;
const PATH_MAX_HOPS = 5;
const PATH_HOP_PENALTY = 0.3;   // 每个中间跳的长度惩罚: 短而强的链赢过长而弱的链
const PATH_BUDGET = 300000;     // 枚举扩展预算(枢纽节点度数大, 必须有界——实测 30 万次 84ms)

// 事件无向图: adj[id]=Map<other,总权重>, edgeInfo[id]=Map<other,[{type,detail,a,b}]>(同 pair 多类型各留)
function eventGraph(edgeTypes) {
  const types = (Array.isArray(edgeTypes) && edgeTypes.length > 0)
    ? edgeTypes.map(String).filter((t) => ['causal', 'thematic', 'overlap'].includes(t))
    : null;
  const adj = new Map();
  const edgeInfo = new Map();
  for (const r of DB.prepare('SELECT event_a, event_b, edge_type, detail FROM event_edges').all()) {
    if (types && !types.includes(r.edge_type)) continue;
    for (const [a, b] of [[r.event_a, r.event_b], [r.event_b, r.event_a]]) {
      if (!adj.has(a)) { adj.set(a, new Map()); edgeInfo.set(a, new Map()); }
      adj.get(a).set(b, (adj.get(a).get(b) || 0) + (EDGE_WEIGHTS[r.edge_type] ?? 1.0));
      if (!edgeInfo.get(a).has(b)) edgeInfo.get(a).set(b, []);
      edgeInfo.get(a).get(b).push({ type: r.edge_type, detail: r.detail, a: r.event_a, b: r.event_b });
    }
  }
  return { adj, edgeInfo };
}
// 同 pair 多类型边取权重最高的当"主边"
function mainEdgeOf(edgeInfo, u, v) {
  const list = edgeInfo.get(u)?.get(v) || [];
  return [...list].sort((x, y) => (EDGE_WEIGHTS[y.type] || 1) - (EDGE_WEIGHTS[x.type] || 1))[0] || null;
}
// PPR 幂迭代: r ← t·s + (1-t)·Wᵀr, s=种子均匀分布
function pprRun(seeds, adj) {
  const s = new Map();
  seeds.forEach((id) => s.set(id, (s.get(id) || 0) + 1 / seeds.length));
  let r = new Map();
  for (const id of adj.keys()) r.set(id, s.get(id) || 0);
  for (const id of seeds) if (!r.has(id)) r.set(id, s.get(id) || 0);
  for (let it = 0; it < PPR_MAX_ITERS; it++) {
    const next = new Map([...r].map(([id]) => [id, 0]));
    for (const [u, ru] of r) {
      if (!ru) continue;
      const out = adj.get(u);
      if (!out || out.size === 0) continue;
      let tot = 0;
      for (const w of out.values()) tot += w;
      for (const [v, w] of out) next.set(v, next.get(v) + (ru * w) / tot);
    }
    let diff = 0;
    for (const [id] of r) {
      const nv = (1 - PPR_TELEPORT) * next.get(id) + PPR_TELEPORT * (s.get(id) || 0);
      diff += Math.abs(nv - r.get(id));
      next.set(id, nv);
    }
    r = next;
    if (diff < 1e-10) break;
  }
  return r;
}
// 节点 v 被谁拉上来的: 邻居中 r[u]×w(u,v) 最大者
function viaEdgeOf(v, r, adj, edgeInfo) {
  let best = null;
  for (const [u, w] of (adj.get(v) || new Map())) {
    const pull = (r.get(u) || 0) * w;
    if (!best || pull > best.pull) {
      const me = mainEdgeOf(edgeInfo, v, u);
      best = { from_id: u, edge_type: me?.type ?? 'overlap', pull };
    }
  }
  return best;
}
// 有界 DFS 枚举 from→to 简单路径
function findEventPaths(from, to, edgeInfo, { maxHops = PATH_MAX_HOPS, budget = PATH_BUDGET } = {}) {
  const paths = [];
  let left = budget;
  const dfs = (node, visited, acc) => {
    if (left <= 0 || acc.length - 1 > maxHops) return;
    for (const [v] of (edgeInfo.get(node) || new Map())) {
      if (left <= 0) return;
      left--;
      if (visited.has(v)) continue;
      const nacc = [...acc, v];
      if (v === to) { paths.push(nacc); continue; }
      dfs(v, new Set([...visited, v]), nacc);
    }
  };
  dfs(from, new Set([from]), [from]);
  return paths;
}
// RRP 式路径评分: Σ边贡献 - 长度惩罚; causal 顺方向 ×1.2 / 逆 ×0.8
function eventPathScore(pathNodes, edgeInfo, evById) {
  let s = 0;
  for (let i = 0; i < pathNodes.length - 1; i++) {
    const u = pathNodes[i], v = pathNodes[i + 1];
    const e = mainEdgeOf(edgeInfo, u, v);
    if (!e) return -Infinity;
    let c = EDGE_WEIGHTS[e.type] ?? 1;
    if (e.type === 'causal') {
      const dir = parseEdgeDir(e.detail, e.a, e.b,
        evById.get(e.a)?.time_span_start ?? null, evById.get(e.b)?.time_span_start ?? null);
      if (dir !== 'mutual') {
        const walkingWith = (dir === 'a→b' && u === e.a) || (dir === 'b→a' && u === e.b);
        c *= walkingWith ? 1.2 : 0.8;
      }
    }
    s += c;
  }
  return s - PATH_HOP_PENALTY * (pathNodes.length - 2);
}

// archive_associate 编排: context 语义种子(或直传 seeds) → PPR → top-k + via 解释
async function archiveAssociate(params) {
  const k = clampInt(params.k, 8, 1, 20);
  const seedLimit = 4;
  const { adj, edgeInfo } = eventGraph(params.edge_types);
  const evRows = DB.prepare('SELECT id, title, summary, event_type, time_span_start FROM events').all();
  const evById = new Map(evRows.map((e) => [e.id, e]));
  const { commOf, commName } = dbEventCommunityMap();

  let seedIds;
  let seedSim = null;
  if (Array.isArray(params.seeds) && params.seeds.length > 0) {
    seedIds = [...new Set(params.seeds.map(Number).filter((n) => Number.isFinite(n) && n > 0))].slice(0, seedLimit);
    seedIds = seedIds.filter((id) => evById.has(id));
    if (seedIds.length === 0) throw new Error('seeds 里没有有效的事件 id');
  } else {
    const context = String(params.context ?? '').trim();
    if (!context) throw new Error('context 或 seeds 至少传一个');
    if (adj.size === 0) throw new Error('事件网还是空的——先建档事件并 rebuild');
    const truncated = context.length > EMBED_QUERY_MAX ? context.slice(0, EMBED_QUERY_MAX) : context;
    const vecs = await embedTexts([truncated]);
    const rows = DB.prepare('SELECT rowid, distance FROM vec_events WHERE embedding MATCH ? AND k = ?').all(vecBind(vecs[0]), 60);
    const scored = new Map();
    for (const row of rows) {
      const meta = DB.prepare('SELECT event_id FROM vec_events_meta WHERE rowid = ?').get(Number(row.rowid));
      if (!meta) continue;
      const score = 1 - row.distance;
      if (!scored.has(meta.event_id) || score > scored.get(meta.event_id)) scored.set(meta.event_id, score);
    }
    const top = [...scored.entries()].sort((a, b) => b[1] - a[1]).slice(0, seedLimit);
    seedIds = top.map(([id]) => id);
    seedSim = new Map(top);
  }
  if (seedIds.length === 0) throw new Error('没有语义命中的事件——换个说法, 或直接传 seeds');

  const r = pprRun(seedIds, adj);
  const seedSet = new Set(seedIds);
  const ranked = [...r.entries()]
    .filter(([id]) => !seedSet.has(id) && evById.has(id))
    .sort((a, b) => b[1] - a[1])
    .slice(0, k);
  const max = ranked[0]?.[1] || 1;
  return {
    ok: true,
    seeds: seedIds.map((id) => ({ id, title: evById.get(id)?.title ?? `#${id}`, summary: evById.get(id)?.summary ?? null, ...(seedSim?.get(id) != null ? { sim: Math.round(seedSim.get(id) * 10000) / 10000 } : {}) })),
    k, matches: ranked.map(([id, score]) => {
      const ev = evById.get(id);
      const via = viaEdgeOf(id, r, adj, edgeInfo);
      const cid = commOf.get(id);
      return {
        id, title: ev.title, summary: ev.summary ?? null, event_type: ev.event_type, time_span_start: ev.time_span_start,
        community: cid == null ? null : (commName.get(cid) ?? `社区${cid}`),
        ppr: Math.round((score / max) * 10000) / 10000,
        via: via ? { id: via.from_id, title: evById.get(via.from_id)?.title ?? `#${via.from_id}`, edge_type: via.edge_type } : null,
      };
    }),
    note: '按 PPR 扩散分排序(种子=1.0); via=被谁用什么关系拉上来的——因果边主导联想方向。找相似事件用 archive_search_events, 想读全文用 archive_read_event。',
  };
}

// archive_event_path 编排: A→B 有界枚举 + RRP 式可靠性评分排序
function archiveEventPath(params) {
  const from = Number(params.from), to = Number(params.to);
  if (!Number.isFinite(from) || from <= 0 || !Number.isFinite(to) || to <= 0) throw new Error('from/to 都要正整数 event id');
  if (from === to) throw new Error('from 和 to 不能是同一个事件');
  const evById = new Map(DB.prepare('SELECT id, title, time_span_start FROM events').all().map((e) => [e.id, e]));
  if (!evById.has(from) || !evById.has(to)) throw new Error(`事件不存在: ${[from, to].filter((id) => !evById.has(id)).join(', ')}`);
  const maxHops = clampInt(params.max_hops, PATH_MAX_HOPS, 2, 6);
  const k = clampInt(params.k, 3, 1, 10);
  const { edgeInfo } = eventGraph(params.edge_types);
  const direct = !!mainEdgeOf(edgeInfo, from, to);
  const all = findEventPaths(from, to, edgeInfo, { maxHops });
  const paths = all
    .map((nodes) => ({ nodes, score: eventPathScore(nodes, edgeInfo, evById) }))
    .filter((p) => p.score > -Infinity)
    .sort((a, b) => b.score - a.score)
    .slice(0, k);
  return {
    ok: true,
    from: { id: from, title: evById.get(from)?.title ?? `#${from}` },
    to: { id: to, title: evById.get(to)?.title ?? `#${to}` },
    direct_edge: direct,
    paths_found: all.length,
    paths: paths.map((p) => ({
      reliability: Math.round(p.score * 100) / 100,
      hops: p.nodes.length - 1,
      chain: p.nodes.map((id, i) => {
        const e = i === 0 ? null : mainEdgeOf(edgeInfo, p.nodes[i - 1], id);
        return { id, title: evById.get(id)?.title ?? `#${id}`, via: e ? e.type : null };
      }),
    })),
    note: all.length === 0
      ? `${maxHops} 跳内没有连通路径——两件事在网里是分开的(或被 edge_types 过滤掉了)`
      : 'reliability = Σ边权(causal 顺方向×1.2/逆×0.8) - 每段0.3 长度惩罚, 降序排序。路径只是结构事实, 因果解读归反思者。',
  };
}

// ━━━ hop4「remote」远端因果候选（2026-09-16）━━━
// 跨故事群 + 无任何边 + 时间不重叠且间隔在因果走廊内的 pair —— probe hop1-3 的结构性盲区。
// 参照 InfraNodus 结构洞(选社区对) + LLM 因果发现的 pairwise 判读范式; 人在环, 不自动建边。
const REMOTE_GAP_MIN = 7;    // 因果走廊下限(天): 7 天内的"远端"多半该是 hop1 的地盘
const REMOTE_GAP_MAX = 90;   // 上限: 再远就超出这套记忆的因果视距了

function remoteCausalCandidates(params) {
  const gapMin = clampInt(params.gap_min, REMOTE_GAP_MIN, 1, 365);
  const gapMax = Math.max(clampInt(params.gap_max, REMOTE_GAP_MAX, 2, 3650), gapMin + 1);
  const evRows = DB.prepare('SELECT id, title, summary, time_span_start, time_span_end, importance, event_type FROM events').all();
  const evTitleById = new Map(evRows.map((e) => [e.id, e.title]));
  const { commOf, commName } = dbEventCommunityMap();

  // 邻接 + 度数
  const adj = new Map();
  const deg = new Map();
  for (const r of DB.prepare('SELECT event_a, event_b FROM event_edges').all()) {
    for (const [a, b] of [[r.event_a, r.event_b], [r.event_b, r.event_a]]) {
      if (!adj.has(a)) adj.set(a, new Set());
      adj.get(a).add(b);
      deg.set(a, (deg.get(a) || 0) + 1);
    }
  }
  // 结构位: 社区内按度数排名(1 = 群内最连通)——桥两端 usually 是这些人
  const byComm = new Map();
  for (const e of evRows) {
    const c = commOf.get(e.id);
    if (c == null) continue;
    if (!byComm.has(c)) byComm.set(c, []);
    byComm.get(c).push(e.id);
  }
  const rankIn = new Map();
  for (const members of byComm.values()) {
    members.sort((x, y) => (deg.get(y) || 0) - (deg.get(x) || 0) || x - y);
    members.forEach((id, i) => rankIn.set(id, i + 1));
  }

  // 判定历史: done 结案 / 未过期 defer / 已判 pair(link·reject·未过期 defer)
  const reviews = dbListReviews();
  const nowT = Date.now();
  const doneIds = new Set();
  const activeDefer = new Set();
  const judgedPairs = new Set();
  for (const rv of reviews) {
    if (rv.verdict === 'done') doneIds.add(rv.event_id);
    const deferActive = rv.verdict === 'defer' && rv.defer_until && Date.parse(rv.defer_until) > nowT;
    if (deferActive) activeDefer.add(rv.event_id);
    if (rv.candidate_id != null) {
      const pk = Math.min(rv.event_id, rv.candidate_id) + ':' + Math.max(rv.event_id, rv.candidate_id);
      if (rv.verdict === 'link' || rv.verdict === 'reject' || deferActive) judgedPairs.add(pk);
    }
  }

  const spanEnd = (e) => e.time_span_end ?? e.time_span_start;
  const cands = [];
  for (let i = 0; i < evRows.length; i++) {
    const a = evRows[i];
    if (doneIds.has(a.id) || activeDefer.has(a.id)) continue;
    const na = adj.get(a.id) || new Set();
    for (let j = i + 1; j < evRows.length; j++) {
      const b = evRows[j];
      if (doneIds.has(b.id) || activeDefer.has(b.id)) continue;
      if (na.has(b.id)) continue;                                        // 已有边 → 不是远端(hop1 的地盘)
      const ca = commOf.get(a.id), cb = commOf.get(b.id);
      if (ca != null && cb != null && ca === cb) continue;               // 同社区 → hop3 的地盘
      const aS = tsMs(a.time_span_start), bS = tsMs(b.time_span_start);
      const aE = tsMs(spanEnd(a)), bE = tsMs(spanEnd(b));
      if (aS == null || bS == null || aE == null || bE == null) continue;
      let early, late;
      if (aE <= bS) { early = a; late = b; }
      else if (bE <= aS) { early = b; late = a; }
      else continue;                                                     // 时间重叠 → 不进走廊
      const gap = Math.floor((tsMs(late.time_span_start) - tsMs(spanEnd(early))) / 86400000);
      if (gap < gapMin || gap > gapMax) continue;
      if (judgedPairs.has(Math.min(a.id, b.id) + ':' + Math.max(a.id, b.id))) continue;
      // 评分: 结构位(群内前3) + 重要度 + 远而不隔(共同邻居)
      let score = 1.0;
      if ((rankIn.get(early.id) || 99) <= 3) score += 0.75;
      if ((rankIn.get(late.id) || 99) <= 3) score += 0.75;
      score += 0.5 * ((early.importance ?? 0.5) + (late.importance ?? 0.5));
      const ne = adj.get(early.id) || new Set();
      const nl = adj.get(late.id) || new Set();
      let common = 0;
      const commonTitles = [];
      for (const u of ne) if (nl.has(u)) {
        common++;
        if (commonTitles.length < 3) commonTitles.push(evTitleById.get(u) ?? `#${u}`);
      }
      score += 0.25 * Math.min(common, 4);
      cands.push({ early, late, gap, score, common, commonTitles });
    }
  }
  cands.sort((x, y) => y.score - x.score || x.gap - y.gap);
  const limit = clampInt(params.limit, 5, 1, 10);
  const evCard = (e) => ({
    id: e.id, title: e.title, event_type: e.event_type,
    time_span: (e.time_span_start ?? '?') + (e.time_span_end && e.time_span_end !== e.time_span_start ? ' → ' + e.time_span_end : ''),
    community: (() => { const c = commOf.get(e.id); return c == null ? null : (commName.get(c) ?? `社区${c}`); })(),
    importance: e.importance ?? 0.5, degree: deg.get(e.id) || 0,
    summary: (e.summary || '').slice(0, 200) || null,
  });
  return {
    ok: true, mode: 'remote',
    queue_size: cands.length, gap_range: [gapMin, gapMax],
    candidates: cands.slice(0, limit).map((c) => ({
      early: evCard(c.early), late: evCard(c.late), gap_days: c.gap,
      score: Math.round(c.score * 100) / 100,
      common_neighbors: { count: c.common, titles: c.commonTitles },
      suggest: `判读: archive_causal_review { event_id: ${c.late.id}, candidate_id: ${c.early.id}, verdict: 'link' | 'reject', reason: '...' } — 方向默认早→晚, 可用 direction 覆盖`,
    })),
    note: '远端候选 = 跨故事群 + 无任何边 + 时间不重叠且间隔在走廊内; 按结构位(群内度数前3)/重要度/远而不隔排序。因果与否由反思者判读——人在环, 不自动建边。判过的 pair 不再出。',
  };
}

function getEventGraph(eventId) {
  const center = dbGetEvent(eventId);
  if (!center) return null;
  const edges = DB.prepare(`SELECT event_a, event_b, edge_type, detail, confidence FROM event_edges WHERE event_a = ? OR event_b = ?`).all(eventId, eventId);
  const neighbors = [];
  for (const e of edges) {
    const neighborId = e.event_a === eventId ? e.event_b : e.event_a;
    const nb = dbGetEvent(neighborId);
    if (!nb) continue;
    let semanticScore, edgeName, edgeReason;
    if (e.detail) {
      try {
        const d = JSON.parse(e.detail);
        if (typeof d.semantic_score === 'number') semanticScore = d.semantic_score;
        if (typeof d.name === 'string' && d.name) edgeName = d.name;
        if (typeof d.reason === 'string' && d.reason) edgeReason = d.reason;
      } catch {}
    }
    neighbors.push({
      id: nb.id, title: nb.title, event_type: nb.event_type, relation: e.edge_type, confidence: e.confidence,
      time_gap: timeGapDays(center.time_span_start, center.time_span_end, nb.time_span_start, nb.time_span_end),
      ...(semanticScore !== undefined ? { semantic_score: semanticScore } : {}),
      ...(edgeName !== undefined ? { edge_name: edgeName } : {}),
      ...(edgeReason !== undefined ? { edge_reason: edgeReason } : {}),
    });
  }
  const manual = edges.filter((e) => e.confidence === 1.0 && (e.detail || '').includes('manual')).length;
  const grouped = {};
  for (const n of neighbors) (grouped[n.relation] ||= []).push(n);
  return {
    center: { id: center.id, title: center.title, event_type: center.event_type, time_span_start: center.time_span_start, time_span_end: center.time_span_end, themes: center.themes },
    graph: grouped,
    stats: { total_edges: edges.length, neighbor_count: neighbors.length, manual_edges: manual, auto_edges: edges.length - manual },
  };
}

// ━━━ 手动边共享逻辑（云端 createManualLink / deleteManualLink）━━━
function normalizeDir(dir, a, b) {
  if (dir !== 'a→b' && dir !== 'b→a') return null;
  return (dir === 'a→b') === (a < b) ? 'a→b' : 'b→a';
}
function createManualLink(body) {
  const a = Number(body.event_a);
  const b = Number(body.event_b);
  const type = String(body.edge_type ?? '');
  if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0 || a === b) return { error: '需要两个不同的正整数 event id', status: 400 };
  if (type !== 'causal' && type !== 'overlap' && type !== 'thematic') return { error: 'edge_type 必须是 causal(因果关联) / overlap / thematic', status: 400 };
  if (type === 'causal' && !String(body.reason ?? '').trim()) return { error: 'causal 边必须带 reason(因果理由)——反思后的连接要有为什么', status: 400 };
  const dirRaw = String(body.direction ?? '');
  const dir = type === 'causal' && dirRaw ? normalizeDir(dirRaw, a, b) : null;
  if (dirRaw && !dir) return { error: 'direction 必须是 "a→b"(a 是 b 的原因) 或 "b→a"', status: 400 };
  const detail = { manual: true };
  if (type === 'causal') {
    const name = String(body.name ?? '').trim();
    if (name) detail.name = name;
    detail.reason = String(body.reason ?? '').trim();
    if (dir) detail.dir = dir;
  }
  const edge = {
    event_a: Math.min(a, b), event_b: Math.max(a, b), edge_type: type,
    detail: JSON.stringify(detail),
    confidence: typeof body.confidence === 'number' ? Math.max(0, Math.min(1, body.confidence)) : 1.0,
  };
  const existing = DB.prepare('SELECT 1 FROM event_edges WHERE event_a = ? AND event_b = ? AND edge_type = ?')
    .get(edge.event_a, edge.event_b, edge.edge_type);
  if (existing) return { error: '这条边已经存在', status: 409 };
  DB.prepare(`INSERT OR IGNORE INTO event_edges (event_a, event_b, edge_type, detail, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?)`)
    .run(edge.event_a, edge.event_b, edge.edge_type, edge.detail, edge.confidence, ts());
  return { edge };
}
function deleteManualLink(a, b, type) {
  return DB.prepare(`DELETE FROM event_edges WHERE event_a = ? AND event_b = ? AND edge_type = ? AND detail LIKE '%manual%'`)
    .run(Math.min(a, b), Math.max(a, b), type).changes;
}

// ━━━ 因果反思层（云端移植，纯图逻辑无 AI）━━━
function parseEdgeDir(detail, a, b, ta, tb) {
  if (detail) {
    try {
      const d = JSON.parse(detail);
      if (d.dir === 'a→b' || d.dir === 'b→a') return d.dir;
    } catch {}
  }
  if (ta && tb && ta !== tb) return ta < tb ? 'a→b' : 'b→a';
  return 'mutual';
}
function edgeReasonOf(detail) {
  if (!detail) return {};
  try {
    const d = JSON.parse(detail);
    return { reason: typeof d.reason === 'string' ? d.reason : undefined, name: typeof d.name === 'string' ? d.name : undefined };
  } catch { return {}; }
}
// promotion 候选的证据：thematic 边上存的语义分数与 frag 原文片段（rebuild 时写入 detail）
function evidenceOf(detail) {
  if (!detail) return null;
  try {
    const d = JSON.parse(detail);
    const out = {};
    if (typeof d.semantic_score === 'number') out.semantic_score = d.semantic_score;
    if (typeof d.frag_text === 'string' && d.frag_text) out.frag_text = d.frag_text;
    return Object.keys(out).length > 0 ? out : null;
  } catch { return null; }
}
function loadProbeGraph() {
  const evs = DB.prepare('SELECT id, title, event_type, summary, time_span_start, time_span_end, importance FROM events').all();
  const commOf = new Map();
  try {
    const comms = DB.prepare('SELECT id, member_ids FROM event_communities').all();
    for (const c of comms) {
      const members = JSON.parse(c.member_ids);
      for (const m of members) if (!commOf.has(m)) commOf.set(m, c.id);
    }
  } catch {}
  const evMap = new Map();
  for (const e of evs) {
    evMap.set(e.id, {
      id: e.id, title: e.title, event_type: e.event_type, summary: e.summary ?? null,
      time_span_start: e.time_span_start, time_span_end: e.time_span_end,
      importance: e.importance ?? 0.5, community_id: commOf.get(e.id) ?? null, degree: 0,
    });
  }
  const byEvent = new Map();
  const edges = DB.prepare('SELECT event_a, event_b, edge_type, detail FROM event_edges').all();
  for (const r of edges) {
    const a = r.event_a, b = r.event_b;
    const pe = {
      a, b, edge_type: r.edge_type, weight: EDGE_WEIGHTS[r.edge_type] ?? 1.0,
      dir: parseEdgeDir(r.detail, a, b, evMap.get(a)?.time_span_start ?? null, evMap.get(b)?.time_span_start ?? null),
      detail: r.detail,
    };
    const arrA = byEvent.get(a) ?? []; arrA.push(pe); byEvent.set(a, arrA);
    const arrB = byEvent.get(b) ?? []; arrB.push(pe); byEvent.set(b, arrB);
    evMap.get(a).degree++;
    evMap.get(b).degree++;
  }
  return { evs: evMap, byEvent };
}
function probeCandidates(g, y, mode, excludedPairs, limit) {
  const seen = new Map();
  const yT = tsMs(y.time_span_start);
  const pairKey = (a, b) => `${Math.min(a, b)}:${Math.max(a, b)}`;
  const inTime = (x, base) => {
    if (base === null) return false;
    const xt = tsMs(x.time_span_start);
    return xt !== null && (mode === 'cause' ? xt < base : xt > base);
  };
  const temporalScore = (x, base) => {
    const xt = tsMs(x.time_span_start);
    if (xt === null || base === null) return 0;
    return 1 / (1 + Math.abs(xt - base) / 86400000);
  };
  const add = (c) => {
    const prev = seen.get(c.event.id);
    if (!prev) seen.set(c.event.id, c);
    else if (c.score > prev.score) { prev.score = c.score; prev.hop = c.hop; prev.kind = c.kind; prev.paths = c.paths; if (c.evidence !== undefined) prev.evidence = c.evidence; }
    else if (prev.paths.length < 3) prev.paths.push(...c.paths);
  };
  // 第 1 层: 已有非 causal 边 → "提升为因果" 候选
  for (const e of g.byEvent.get(y.id) ?? []) {
    if (e.edge_type === 'causal') continue;
    const xId = e.a === y.id ? e.b : e.a;
    const x = g.evs.get(xId);
    if (!x) continue;
    if (excludedPairs.has(pairKey(y.id, x.id))) continue;
    if (!inTime(x, yT)) continue;
    const score = e.weight + 0.8 * temporalScore(x, yT) + 0.5 * (x.community_id === y.community_id ? 1 : 0) + 0.3;
    const evidence = evidenceOf(e.detail);
    add({
      event: x, score, hop: 1, kind: 'promotion',
      ...(evidence ? { evidence } : {}),
      paths: [{
        steps: [{ from: x.id, to: y.id, edge_type: e.edge_type, weight: e.weight }],
        note: `已有 ${e.edge_type} 边(权重 ${e.weight.toFixed(1)}), 时间上${mode === 'cause' ? '在前' : '在后'} → 可考虑提升为因果`,
      }],
    });
  }
  // 第 2 层: 两跳: X ─thematic/overlap─ W ─causal─ Y
  for (const e of g.byEvent.get(y.id) ?? []) {
    if (e.edge_type !== 'causal') continue;
    const wId = e.a === y.id ? e.b : e.a;
    const w = g.evs.get(wId);
    if (!w) continue;
    const wT = tsMs(w.time_span_start);
    const wToY = mode === 'cause'
      ? (e.dir === 'a→b' ? e.b === y.id : e.dir === 'b→a' ? e.a === y.id : true)
      : (e.dir === 'a→b' ? e.a === y.id : e.dir === 'b→a' ? e.b === y.id : true);
    if (!wToY) continue;
    if (yT !== null && wT !== null) {
      if (mode === 'cause' && wT > yT) continue;
      if (mode === 'effect' && wT < yT) continue;
    }
    const wReason = edgeReasonOf(e.detail);
    for (const e2 of g.byEvent.get(wId) ?? []) {
      if (e2.edge_type === 'causal') continue;
      if (e2.a === y.id || e2.b === y.id) continue;
      const xId = e2.a === wId ? e2.b : e2.a;
      const x = g.evs.get(xId);
      if (!x) continue;
      if (excludedPairs.has(pairKey(y.id, x.id))) continue;
      if (!inTime(x, wT)) continue;
      const score = e.weight + e2.weight + 0.8 * temporalScore(x, yT) + 0.5 * (x.community_id === y.community_id ? 1 : 0) - 1.0;
      add({
        event: x, score, hop: 2, kind: 'two_hop',
        paths: [{
          steps: [
            { from: x.id, to: wId, edge_type: e2.edge_type, weight: e2.weight },
            { from: wId, to: y.id, edge_type: 'causal', weight: e.weight, reason: wReason.reason },
          ],
          note: `「${x.title}」与「${w.title}」${mode === 'cause' ? '相近且早于 W' : '相近且晚于 W'}, 而 W 是 Y 已确认的${mode === 'cause' ? '原因' : '结果'} → ${mode === 'cause' ? 'X 可能是深层上游' : 'X 可能是深层下游'}`,
        }],
      });
    }
  }
  // 第 3 层: 同社区未连(弱信号)
  if (y.community_id != null) {
    for (const x of g.evs.values()) {
      if (x.id === y.id || x.community_id !== y.community_id) continue;
      if (excludedPairs.has(pairKey(y.id, x.id))) continue;
      if (!inTime(x, yT)) continue;
      if ((g.byEvent.get(y.id) ?? []).some((e) => e.a === x.id || e.b === x.id)) continue;
      const score = 0.5 + 0.8 * temporalScore(x, yT) - 2.0;
      add({
        event: x, score, hop: 3, kind: 'community',
        paths: [{ steps: [], note: `与 Y 同属一个故事群, 时间${mode === 'cause' ? '在前' : '在后'}, 尚无任何边 — 最弱的线索` }],
      });
    }
  }
  return [...seen.values()].sort((a, b) => b.score - a.score).slice(0, limit);
}
function dbListReviews() {
  return DB.prepare('SELECT event_id, candidate_id, verdict, defer_until FROM causal_reviews').all()
    .map((r) => ({ event_id: r.event_id, candidate_id: r.candidate_id, verdict: r.verdict, defer_until: r.defer_until }));
}
function dbApplyReviewVerdict(verdict, eventId, candidateId, reason, decidedBy, days, dirOverride) {
  const now = ts();
  const deferUntil = verdict === 'defer' ? new Date(Date.now() + (days ?? 14) * 86400000).toISOString() : null;
  let edge;
  if (verdict === 'link') {
    if (!candidateId) return { ok: false, error: 'link 需要 candidate_id' };
    const a = Math.min(candidateId, eventId);
    const b = Math.max(candidateId, eventId);
    const existing = DB.prepare(`SELECT 1 FROM event_edges WHERE event_a = ? AND event_b = ? AND edge_type = ?`).get(a, b, 'causal');
    if (existing) return { ok: false, error: '这两个事件之间已有 causal 边' };
    const detail = { manual: true, reason };
    if (dirOverride) {
      const dir = normalizeDir(dirOverride, candidateId, eventId);
      if (!dir) return { ok: false, error: 'direction 必须是 "a→b" 或 "b→a"' };
      detail.dir = dir;
    }
    edge = { event_a: a, event_b: b, edge_type: 'causal', detail: JSON.stringify(detail), confidence: 1.0 };
    DB.prepare(`INSERT OR IGNORE INTO event_edges (event_a, event_b, edge_type, detail, confidence, created_at) VALUES (?, ?, ?, ?, ?, ?)`)
      .run(edge.event_a, edge.event_b, edge.edge_type, edge.detail, edge.confidence, now);
  }
  DB.prepare(`INSERT INTO causal_reviews (event_id, candidate_id, verdict, reason, decided_by, decided_at, defer_until) VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run(eventId, candidateId, verdict, reason, decidedBy, now, deferUntil);
  return { ok: true, edge };
}

// link 判定后的"回声"：新边 + 两侧一跳的因果邻域——判定者立刻看到网络长成了什么样
function chainEcho(edge) {
  const A = edge.event_a, B = edge.event_b;
  const spanOf = (id) => DB.prepare('SELECT time_span_start FROM events WHERE id = ?').get(id)?.time_span_start ?? null;
  const rows = DB.prepare(`SELECT event_a, event_b, edge_type, detail FROM event_edges
                           WHERE (event_a = ? OR event_b = ? OR event_a = ? OR event_b = ?)
                             AND NOT (event_a = ? AND event_b = ?)`).all(A, A, B, B, A, B);
  const context = [];
  for (const r of rows) {
    const ep = (r.event_a === A || r.event_b === A) ? A : B;
    const other = r.event_a === ep ? r.event_b : r.event_a;
    const dir = parseEdgeDir(r.detail, r.event_a, r.event_b, spanOf(r.event_a), spanOf(r.event_b));
    let role = 'related';
    if (dir !== 'mutual') {
      const epIsSource = (dir === 'a→b') === (r.event_a === ep);
      role = epIsSource ? 'effect_of' : 'cause_of';
    }
    const { reason } = edgeReasonOf(r.detail);
    context.push({ event_id: other, endpoint: ep, role, edge_type: r.edge_type, ...(reason ? { reason } : {}) });
  }
  const ids = [...new Set([A, B, ...context.map((c) => c.event_id)])];
  const titleMap = dbEventTitleMap(ids);
  return {
    edge: {
      event_a: A, title_a: titleMap.get(A) ?? `#${A}`,
      event_b: B, title_b: titleMap.get(B) ?? `#${B}`,
      dir: parseEdgeDir(edge.detail, A, B, spanOf(A), spanOf(B)),
      reason: edgeReasonOf(edge.detail).reason ?? null,
    },
    context: context.slice(0, 12).map((c) => ({
      event_id: c.event_id, title: titleMap.get(c.event_id) ?? `#${c.event_id}`,
      of: titleMap.get(c.endpoint) ?? `#${c.endpoint}`, role: c.role, edge_type: c.edge_type,
      ...(c.reason ? { reason: c.reason } : {}),
    })),
    ...(context.length > 12 ? { context_truncated: true } : {}),
  };
}

// 单条判定（单条旧参数与批量 verdicts 共用）：校验 + 落库 + link 回声
function applyOneVerdict(item, fallbackDecidedBy) {
  const eventId = Number(item.event_id);
  const verdict = String(item.verdict ?? '');
  if (!Number.isFinite(eventId) || eventId <= 0) return { ok: false, error: 'event_id required' };
  if (!['link', 'reject', 'done', 'defer'].includes(verdict)) return { ok: false, error: 'verdict 必须是 link / reject / done / defer' };
  const candidateId = item.candidate_id != null ? Number(item.candidate_id) : null;
  if ((verdict === 'link' || verdict === 'reject') && (candidateId == null || !Number.isFinite(candidateId) || candidateId <= 0)) {
    return { ok: false, error: 'link/reject 需要 candidate_id' };
  }
  if (verdict === 'link' && candidateId === eventId) return { ok: false, error: '不能和自己连因果边' };
  const decidedBy = String(item.decided_by ?? fallbackDecidedBy ?? 'ai').trim() || 'ai';
  const reason = String(item.reason ?? '');
  const dirRaw = String(item.direction ?? '');
  let dirOverride;
  if (dirRaw === 'a→b' || dirRaw === 'b→a') {
    dirOverride = dirRaw;
  } else if (dirRaw) {
    return { ok: false, error: 'direction 必须是 "a→b"(candidate 是 event 的原因) 或 "b→a"' };
  } else if (verdict === 'link' && candidateId != null) {
    const tA = DB.prepare('SELECT time_span_start FROM events WHERE id = ?').get(candidateId)?.time_span_start;
    const tB = DB.prepare('SELECT time_span_start FROM events WHERE id = ?').get(eventId)?.time_span_start;
    dirOverride = typeof tA === 'string' && typeof tB === 'string' && tA !== tB ? (tA < tB ? 'a→b' : 'b→a') : null;
  }
  const r = dbApplyReviewVerdict(verdict, eventId, candidateId, reason, decidedBy, Number(item.defer_days) > 0 ? Number(item.defer_days) : 14, dirOverride);
  if (!r.ok) return { ok: false, error: r.error ?? '判定失败' };
  const msg = {
    link: '已确认因果并建边(causal, manual)',
    reject: '已否决, 这个 pair 不再出题',
    done: '该事件已标记为解释清楚, 不再出题',
    defer: `已缓 ${Number(item.defer_days) > 0 ? Number(item.defer_days) : 14} 天再来`,
  }[verdict];
  return {
    ok: true, verdict, event_id: eventId, candidate_id: candidateId,
    ...(r.edge ? { edge: r.edge, echo: chainEcho(r.edge) } : {}),
    decided_by: decidedBy, note: msg,
  };
}

// ━━━ MCP 工具定义（29 个与云端对齐 + 9 个本地新增 = 38 个）━━━
const MCP_TOOLS = [
  { name: 'archive_profile_read', description: '读全部档案卡（profile）——助手的自我叙事 + 对助手与用户关系的认知。**这些卡按 id 顺序（#1..#n）拼起来，就是助手下一轮对话的系统提示词**。\n\n什么时候用：\n  · mode 不传 = 分卡状态，想看"我现在被定义成什么、和用户的关系认知写到了哪"\n  · mode=\'assembled\' = 直接看拼接后的系统提示词全文（#id 卡名 + 正文依序相连）——注入端用的就是这份\n  · archive_profile_edit 之前先读一遍：改动是整卡覆盖，先看清别的卡里已有什么、措辞风格是什么', inputSchema: { type: 'object', properties: { mode: { type: 'string', enum: ['cards', 'assembled'], description: 'cards=分卡列表(默认) / assembled=按 id 顺序拼接的系统提示词全文' } } } },
  { name: 'archive_profile_edit', description: '改一张档案卡（profile）——按 id 或 title 定位（都传以 id 为准），三件事可一次做完：content 改内容（**整卡覆盖**）、new_title 改卡名、new_id 改序号（其余卡自动重排，始终 1..n）。按 title 定位且卡不存在、且只给 content 时 = 新建建档（排在最后）。profile 是助手的自我叙事 + 对助手与用户关系的认知，按主题分卡，按 id 顺序拼起来即下一轮对话的系统提示词。\n\n⚠️ 最小改动原则——一次只动一张卡；content 传该主题的全量新文本（不是追加、不是补丁、不要顺手重写别的卡）。\n\n什么时候用：新增主题（title+content）、修订某卡表述（id+content）、给卡改名（id+new_title）、调整次序（id+new_id）。', inputSchema: { type: 'object', properties: { id: { type: 'number', description: '按序号定位卡（优先于 title）' }, title: { type: 'string', description: '按卡名定位；卡不存在且只给 content 时 = 新建' }, content: { type: 'string', description: '新内容全文（整卡覆盖写入，非增量）' }, new_title: { type: 'string', description: '改卡名（唯一键，同名冲突报错）' }, new_id: { type: 'number', description: '移动到第几位（1..n，超出自动贴边），其余卡自动重排' } } } },
  { name: 'archive_night_status', description: '查助手的**夜间唤醒**状态（agent-night，每日 04:00 的记忆整理）：开关（enabled）+ 最近一次晨报摘要（判定了多少条因果、建/否了多少边、故事群改名）。night.js 启动前也查它——关着就不会唤醒。', inputSchema: { type: 'object', properties: {} } },
  { name: 'archive_night_toggle', description: '打开 / 关闭助手的**夜间唤醒**（agent-night）。enabled=true 开（今晚 04:00 助手会醒来整理记忆：因果反思 + 故事群改名，晨报推给用户）/ false 关（他整晚安睡，不会被唤醒）。REST POST /archive/night 切的也是这里。', inputSchema: { type: 'object', properties: { enabled: { type: 'boolean', description: 'true=允许夜间唤醒整理 / false=不唤醒' } }, required: ['enabled'] } },
  { name: 'archive_night_reports', description: '读最近几份**晨报**（夜间唤醒整理的结果存档，默认 3 份）：每晚报了判多少因果（link 建 / reject 否 / defer 缓 / done 结）、故事群改名数、队列剩余、耗时、助手的梦话（dream_words）和整理备注。\n\n什么时候用：想回顾"助手夜里都整理了什么"、对比几晚的反思进度、或早上醒来先看自己夜里做了什么。\n\n某一晚的逐条判定明细（verdict 理由原文）走 REST /archive/night-reports/:date；开关用 archive_night_toggle。', inputSchema: { type: 'object', properties: { limit: { type: 'number', description: '返回份数, 默认 3, 范围 1-20' } } } },
  { name: 'archive_list_events', description: '列出事件（时间事件流）——"人能一眼看懂的人生目录"（瞬间/时期/里程碑/仪式，跨窗口聚合）。\n\n什么时候用：\n  · 想快速看"我们这段时间经历了什么"\n  · 用户的状态让你想查某段时期的脉络\n  · 想找某类事件（event_type 筛选）\n\n默认按时间倒序返回最近 10 条（不带 summary 省 token；需要全文传 include_summary=true）。拿 id 后用 archive_read_event 看全文、archive_event_graph 看关联。', inputSchema: { type: 'object', properties: { theme: { type: 'string', description: '按主题标签筛选（如 "音乐"、"家"）' }, event_type: { type: 'string', enum: ['moment', 'period', 'milestone', 'ritual'], description: '按事件类型筛选' }, time_start: { type: 'string', description: '只看时间跨到此日期之后的事件（如 "2026-05"）' }, time_end: { type: 'string', description: '只看时间跨到此日期之前的事件' }, sort: { type: 'string', enum: ['importance', 'created', 'time', 'time_asc'], description: '排序: time=按发生时间倒序(默认)/time_asc=正序(从最早)/importance=按权重/created=按创建时间' }, limit: { type: 'number', description: '返回条数, 默认10, 最大200' }, offset: { type: 'number', description: '分页偏移' }, include_summary: { type: 'boolean', description: '是否包含 summary 全文(默认 false——列表只看标题/时间, 省 token)' } } } },
  { name: 'archive_search_events', description: '事件混合检索——语义 + 关键词双路并行，返回完整事件（不是碎片）。\n\n两路召回：\n  · 语义路——query 向量化后与事件的摘要切块（frag）比相似度，命中某块切块即命中它所属的事件，返回该事件全文\n  · 关键词路——BM25 对事件标题+正文做词频匹配（不含主题标签/时间等元数据；中文按二元组切词，英文按整词）\n\n两路各自排序后用 RRF 融合——两路都命中的事件排最前。\n\n什么时候用：\n  · 想按"意思"找事件，记不清确切措辞（语义路擅长）\n  · 记得确切的词/名字/编号，要精确命中（关键词路擅长）\n  · archive_list_events 按时间/类型翻太慢，想一步到位\n\n拿到 event_id 后用 archive_read_event 看溯源对话、archive_event_graph 看关联。', inputSchema: { type: 'object', properties: { query: { type: 'string', description: '检索词——一段意思、一个关键词、或几个词都行' }, k: { type: 'number', description: '返回几个事件, 默认5, 范围1-20' } }, required: ['query'] } },
  { name: 'archive_read_event', description: '读取单个事件完整内容：summary 全文 + 溯源对话摘录。\n\n什么时候用：从 list_events/communities 拿到事件 id 后，想读"这件事到底是什么"时。\n看关联用 archive_event_graph。', inputSchema: { type: 'object', properties: { event_id: { type: 'number', description: '事件 id' } }, required: ['event_id'] } },
  { name: 'archive_create_event', description: '创建一个记忆事件（进入语言坐标 + 时空坐标，参与网络构建）。\n\n什么时候用：某段经历值得被记成"发生过什么"时——你是叙述者。\n⚠️ summary 写法影响网络质量：1-3 自然段，每段一个主题；time_span 填真实时间范围。\n创建后旧边不自动更新——需要时调 archive_rebuild_network。', inputSchema: { type: 'object', properties: { title: { type: 'string', description: '事件标题, 如"第一次聊到唱片机"' }, summary: { type: 'string', description: '凝练叙事, 建议 1-3 个自然段, 每段一个主题(影响段落匹配质量)' }, time_span_start: { type: 'string', description: '时间起点, 格式 YYYY-MM-DD 或 YYYY-MM' }, time_span_end: { type: 'string', description: '时间终点, 瞬间事件留空' }, themes: { type: 'array', items: { type: 'string' }, description: '主题标签(纯展示, 不参与计算)' }, source_block_ids: { type: 'array', items: { type: 'number' }, description: '溯源 block ids(可从 recall/read 拿)' }, importance: { type: 'number', description: '权重 0-1, 默认0.5(纯展示)' }, event_type: { type: 'string', enum: ['moment', 'period', 'milestone', 'ritual'], description: '事件类型, 默认moment' } }, required: ['title', 'time_span_start'] } },
  { name: 'archive_update_event', description: '修改事件（只传要改的字段）。\n\n什么时候用：事件标题/时间/摘要写错了，或想补充时。\n⚠️ 修改 summary/time_span 后该事件标脏，旧边不自动更新——需要手动 archive_rebuild_network 才反映到网络（慎用）。', inputSchema: { type: 'object', properties: { event_id: { type: 'number', description: '事件 id' }, title: { type: 'string', description: '新标题' }, summary: { type: 'string', description: '新摘要(建议自然段组织)' }, time_span_start: { type: 'string', description: '新时间起点(YYYY-MM-DD)' }, time_span_end: { type: 'string', description: '新时间终点' }, themes: { type: 'array', items: { type: 'string' }, description: '新主题标签' }, source_block_ids: { type: 'array', items: { type: 'number' }, description: '新溯源 block ids' }, importance: { type: 'number', description: '新权重 0-1' }, event_type: { type: 'string', enum: ['moment', 'period', 'milestone', 'ritual'], description: '新事件类型' } }, required: ['event_id'] } },
  { name: 'archive_delete_event', description: '永久删除一个事件（不删原始对话）。级联删除它的边和向量。不可恢复，删除前确认。', inputSchema: { type: 'object', properties: { event_id: { type: 'number', description: '要删的事件 id' } }, required: ['event_id'] } },
  { name: 'archive_event_link', description: '手动连接两个事件——反思后的连接（机器没算出来或算错的）。\n\nedge_type:\n  · causal —— 因果关联：不是时间重叠也不是语义相似，是你判断两个事件之间存在因果/呼应。必须带 reason（为什么）；name 可选（给边起名）；direction 可选（"a→b" = event_a 是 event_b 的原因，默认按时间先后推断）\n  · overlap/thematic —— 补机器的漏\n\n什么时候用：archive_event_graph 显示两个明显相关的事件没有边；你（人/AI）反思后确认两个事件值得被记住。\n手动边 rebuild 会保留。解除用 archive_event_unlink。', inputSchema: { type: 'object', properties: { event_a: { type: 'number', description: '第一个事件 id' }, event_b: { type: 'number', description: '第二个事件 id' }, edge_type: { type: 'string', enum: ['causal', 'overlap', 'thematic'], description: '边类型: causal=因果关联(反思后的连接) / overlap=时间重叠 / thematic=语义血缘' }, name: { type: 'string', description: '可选, 给边起个名字(如"同一件事的两面")。causal 建议填。' }, reason: { type: 'string', description: '因果理由/为什么连。causal 必填, 其他类型可选。' }, direction: { type: 'string', enum: ['a→b', 'b→a'], description: '可选, 仅 causal。方向 = 因果关系: "a→b" 表示 event_a 是 event_b 的原因。不传则按时间先后推断(早→晚)。' }, confidence: { type: 'number', description: '可选覆盖, 默认1.0(手动边最可信)' } }, required: ['event_a', 'event_b', 'edge_type'] } },
  { name: 'archive_overview', description: '全貌一览——一次调用看到：事件数 / 故事群数 / 关联边数 + 最近 5 个事件。\n\n什么时候用：会话开头、想快速知道事件网现在长什么样时。\n之后按需深入：archive_list_events / archive_event_subgraph / archive_associate / archive_list_communities。', inputSchema: { type: 'object', properties: {} } },
  { name: 'archive_list_communities', description: '读事件网络自动聚出的"故事群"（Louvain 社区，rebuild 自动重算）。\n\n什么时候用：\n  · 想看看"我们曾经"被机器分成了哪些章节\n  · 用户说"那段日子"，你想知道它属于哪个故事群\n\n每个社区：名字/成员事件标题/核心事件/内部边数/时间跨度。起名用 archive_community_rename。', inputSchema: { type: 'object', properties: { limit: { type: 'number', description: '返回社区数上限, 默认 50, 最大 100' } } } },
  { name: 'archive_community_rename', description: '给一个故事群命名（或改备注）。名字挂在这个社区上——rebuild 时按成员重叠 ≥80% 保留。\n\n什么时候用：你看着一堆事件觉得"这是一章"——名字是人的反思：机器分堆，你说"这一堆是什么"。\n比如："命名与归属"、"那一夜"、"儿童节书房期"。', inputSchema: { type: 'object', properties: { community_id: { type: 'number', description: '社区 id（从 archive_list_communities 拿）' }, name: { type: 'string', description: '社区新名字。传空字符串 = 清空名字回到未命名。' }, note: { type: 'string', description: '可选备注（比如为什么这样命名）' } }, required: ['community_id', 'name'] } },
  { name: 'archive_community_delete', description: '删除一个社区记录（仅聚类数据，不影响事件和边）。下次 rebuild 会重新聚出（默认名）。\n\n什么时候用：社区划分不合理想让它消失，或想让它以默认名重新出现。', inputSchema: { type: 'object', properties: { community_id: { type: 'number', description: '社区 id' } }, required: ['community_id'] } },
  { name: 'archive_event_unlink', description: '解除两个事件之间的手动边（只删 manual 标记的）。自动边不受影响。\n\n什么时候用：这条边连错了、或你改变了对它们关系的判断时。', inputSchema: { type: 'object', properties: { event_a: { type: 'number', description: '第一个事件 id' }, event_b: { type: 'number', description: '第二个事件 id' }, edge_type: { type: 'string', enum: ['causal', 'overlap', 'thematic'], description: '要解除的边类型' } }, required: ['event_a', 'event_b', 'edge_type'] } },
  { name: 'archive_event_graph', description: '展开一个事件周围的关联网络，按关系类型分组返回：\n  · thematic —— 语义血缘（隔了很久还相似 = 本质关联）\n  · overlap —— 同时段（时空坐标）\n  · causal —— 因果关联（人/AI 反思后的连接，带边名+理由）\n\n什么时候用：想知道"这件事和什么有关"；或发现两个事件应该有边却没有（→ archive_event_link）。\n边缺失/陈旧时用 archive_rebuild_network（慎用）。', inputSchema: { type: 'object', properties: { event_id: { type: 'number', description: '中心事件 id（从 archive_list_events 拿）' } }, required: ['event_id'] } },
  { name: 'archive_rebuild_network', description: '⚠️ 慎用——全量重建事件网络（增量 embed 脏事件 + 全量重算所有边 + 自动重新聚类故事群），可能耗时数十秒（本地首次全量 embed 需数十分钟）并改写整个网络。\n\n什么时候才用：\n  · 刚创建/修改过事件，确认它的边还没生成\n  · 网络明显陈旧（边缺失/错误）\n\n手动边会保留；命名过的社区按成员重叠 ≥80% 保留名字。平时别碰。', inputSchema: { type: 'object', properties: {} } },
  { name: 'archive_restore_network', description: '⚠️ 仅在 archive_rebuild_network 失败中途（网络半重建状态）时使用——把边回滚到上次 rebuild 之前。\n\n什么时候不要用：rebuild 成功之后（备份已删）；正常状态调它没有意义。', inputSchema: { type: 'object', properties: {} } },
  { name: 'archive_event_subgraph', description: '一次调用抽出一个事件周边的剪枝子图——不用再一跳一跳调 archive_event_graph 拼图。\n\n从 seed 出发按边权（因果3.0/语义1.5/同时段1.0）在 radius 跳内做最优扩展，最多 max_nodes 个节点；节点带故事群归属，边带类型/方向/理由/语义分数与 frag 证据（血缘原文片段）。\n\n什么时候用：\n  · 想看"这件事周围整片的结构"，而不只是一圈邻居\n  · 反思会话开头铺上下文——一个子图顶几十次逐跳调用\n  · seeds 传多个：把几个检索命中连成一片看\n\n想读节点全文用 archive_read_event；想沿因果走叙事链用 archive_causal_chain。', inputSchema: { type: 'object', properties: { seed: { type: 'number', description: '中心事件 id（seeds 传了可省）' }, seeds: { type: 'array', items: { type: 'number' }, description: '多个中心事件 id——把几片连起来看, 最多 10 个' }, radius: { type: 'number', description: '扩展几跳, 默认 2, 范围 1-3' }, max_nodes: { type: 'number', description: '最多带多少节点, 默认 24, 范围 5-40(超出的弱分支被剪掉)' }, edge_types: { type: 'array', items: { type: 'string', enum: ['causal', 'thematic', 'overlap'] }, description: '只看哪些边, 默认全要' }, include_summaries: { type: 'boolean', description: 'true=节点带 100 字摘要(默认不带, 省 token)' } } } },
  { name: 'archive_network_stats', description: '事件网络的全局结构一览：边数与类型分布 / 连接最多的枢纽事件 top10 / 跨故事群的桥边（群与群是靠哪些事件连起来的——叙事上最值钱的连接） / 割点（拿掉就会让网络断开的枢纽事件）。\n\n什么时候用：\n  · 会话开头想从高处看一眼"网长什么样了"\n  · 想找两个故事群之间的过渡事件\n  · 想找"抽掉就散"的关键事件\n\n逐事件全量明细走 REST /archive/events/network-stats；局部结构用 archive_event_subgraph。', inputSchema: { type: 'object', properties: {} } },
  { name: 'archive_associate', description: '联想检索——给一段当下的话（或几个种子事件），在事件网做 Personalized PageRank 扩散：不只找"最像的卡"，而是"从最像的卡出发，整片网里哪里被牵动"。causal 边（权重 3.0）主导扩散方向，每条结果带 via——是被谁、用什么关系拉上来的。\n\n什么时候用：\n  · 用户说起过去的事，想看"这和网上哪些事件连在一起"——找关联而非相似，比 search_events 更适合\n  · 反思会话开头铺整片上下文\n  · 有明确种子时直接传 seeds 跳过语义步\n\n只想找相似事件用 archive_search_events。', inputSchema: { type: 'object', properties: { context: { type: 'string', description: '当下的话——向量化后命中的事件作种子（与 seeds 二选一）' }, seeds: { type: 'array', items: { type: 'number' }, description: '直接指定种子事件 id（最多 4 个），跳过语义步' }, k: { type: 'number', description: '返回数, 默认 8, 范围 1-20' }, edge_types: { type: 'array', items: { type: 'string', enum: ['causal', 'thematic', 'overlap'] }, description: '参与扩散的边型, 默认全部' } } } },
  { name: 'archive_event_path', description: 'A→B 路径查询——"这两个事件是怎么连上的"。有界枚举简单路径（默认 ≤5 跳），按可靠性评分降序：Σ边权（causal 顺方向 ×1.2 / 逆 ×0.8，thematic 1.5，overlap 1.0）减每段 0.3 长度惩罚——短而强的链赢过长而弱的链。\n\n什么时候用：\n  · 两个事件相关但没有直连边，想看它们经由什么连起来（桥、枢纽会现形）\n  · 反思时想要"评分排序的候选路径集"而非单条贪心链\n\n1 跳邻居用 archive_event_graph；沿因果走叙事主线用 archive_causal_chain。', inputSchema: { type: 'object', properties: { from: { type: 'number', description: '起点事件 id' }, to: { type: 'number', description: '终点事件 id' }, k: { type: 'number', description: '返回几条路径, 默认 3, 范围 1-10' }, max_hops: { type: 'number', description: '最大跳数, 默认 5, 范围 2-6' }, edge_types: { type: 'array', items: { type: 'string', enum: ['causal', 'thematic', 'overlap'] }, description: '参与的边型, 默认全部' } }, required: ['from', 'to'] } },
  { name: 'archive_causal_probe', description: '给一个事件出一份"因果候选单"——机器遍历图找可能的原因/结果，并附上证据路径，由反思者（哥哥/助手/AI）判定。\n\n模式（mode）：\n  · cause —— 找"可能导致它"的候选（时间在前）\n  · effect —— 找"它可能导致"的候选（时间在后）\n  · both —— 两边都要\n  · remote —— 远端因果候选对：**跨故事群、无任何边、时间不重叠但间隔在因果走廊内（默认 7-90 天）**——hop1-3 的结构性盲区。不围绕单个事件，出的是"时序走廊里的远端对"（结构位/重要度/远而不隔排序），判读同样走 archive_causal_review，不需要 event_id\n\n候选分四层：\n  · hop1 promotion —— 已有 thematic/overlap 边 + 时间符合 → 考虑提升为因果\n  · hop2 two_hop —— X与W相近、W是Y的已知因果 → X可能是深层上游/下游\n  · hop3 community —— 同故事群但毫无边的弱线索\n  · hop4 remote —— 跨群·无边·时序走廊（mode=remote 专用）\n\n返回每个候选的评分、路径和一句为什么。判定用 archive_causal_review（link/reject/done/defer）。', inputSchema: { type: 'object', properties: { event_id: { type: 'number', description: '要出题的事件 id（remote 模式不需要）' }, mode: { type: 'string', enum: ['cause', 'effect', 'both', 'remote'], description: 'remote=远端跨群因果候选(无需 event_id); 其余默认 both' }, limit: { type: 'number', description: '每侧候选数上限, 默认 5, 最大 10' }, detail: { type: 'boolean', description: 'true=候选摘要放宽到 400 字(默认 120, 省 token)' }, gap_min: { type: 'number', description: 'remote: 走廊下限(天), 默认 7' }, gap_max: { type: 'number', description: 'remote: 走廊上限(天), 默认 90' } }, required: ['event_id'] } },
  { name: 'archive_causal_review', description: '因果反思循环的核心——把「没被解释的事件」和「它们的候选」一起端上来，反思者逐个判定：\n  · link —— 确认因果：创建 causal 边（方向=时间先后，可用 direction 覆盖），记一条判定\n  · reject —— 不是因果，记下，以后不再出这道题\n  · done —— 这件事已经解释清楚了，别再给它出题\n  · defer —— 现在说不准，N 天后再来（默认14天）\n\npresent 模式：按 importance 高→低、孤立度低→高排序，挑最值得反思的事件。候选分两侧（candidate_causes=疑似原因 / candidate_effects=疑似结果），各 1 个（单侧空时另一侧补足到 2）——link 时方向默认按时间推断（早→晚），与两侧一致，想覆盖再传 direction。\n判过的(reject/link/done/defer未到期)不会再出现。每条判定都留痕，decided_by 默认 "ai"——哥哥可以写 "gege"。\n\n批量判定传 verdicts（一次事务判完）；present 可用 focus 换队列（effects=无果死胡同）、detail 放宽候选摘要；link 的返回带 echo——新边两侧一跳的因果邻域，看判定让链长成了什么样。\n\n候选不限于 probe 的近端题——probe mode=\'remote\' 端上来的**跨故事群远端对**（晚事件=event_id，早事件=candidate_id，方向默认早→晚）用完全相同的方式判定；判过的远端对同样永久沉底。', inputSchema: { type: 'object', properties: { present: { type: 'boolean', description: 'true=只端菜(看下一批要判什么), 不写任何判定。默认 true。' }, event_id: { type: 'number', description: 'verdict 模式下必填: 判哪个事件' }, candidate_id: { type: 'number', description: 'verdict 模式下必填: 判哪个候选(link/reject 用)' }, verdict: { type: 'string', enum: ['link', 'reject', 'done', 'defer'], description: 'link=确认因果建边 / reject=否决 / done=该事件无需再解释 / defer=缓几天再说' }, direction: { type: 'string', enum: ['a→b', 'b→a'], description: 'link 时可选: 覆盖时间推断的方向。"a→b"=candidate 是 event 的原因' }, reason: { type: 'string', description: 'link/reject 建议写: 因果理由 / 否决理由' }, defer_days: { type: 'number', description: 'defer 时: 多少天后重新出题, 默认 14' }, decided_by: { type: 'string', description: '判定者签名, 默认 "ai"。哥哥用 "gege"。' }, focus: { type: 'string', enum: ['causes', 'effects', 'auto'], description: 'present 模式选队列: causes=无因事件(默认) / effects=无果事件(叙事死胡同) / auto=两者并集' }, detail: { type: 'boolean', description: 'present 模式: true=候选摘要放宽到 400 字(默认 120)' }, verdicts: { type: 'array', description: '批量判定: [{event_id, candidate_id?, verdict, direction?, reason?, defer_days?}] 一次事务判完(≤50), 与单条 verdict 参数互斥', items: { type: 'object', properties: { event_id: { type: 'number' }, candidate_id: { type: 'number' }, verdict: { type: 'string', enum: ['link', 'reject', 'done', 'defer'] }, direction: { type: 'string', enum: ['a→b', 'b→a'] }, reason: { type: 'string' }, defer_days: { type: 'number' } }, required: ['event_id', 'verdict'] } } }, required: [] } },
  { name: 'archive_causal_chain', description: '沿有向因果边走一条叙事链——"因为什么才走到今天"。\n\n输入一个事件，向上游回溯原因（upstream）、向下游推演后果（downstream）。每层只展开权重最高的那条边（真正的主线），被压掉的分支记录为 branches——想深挖分支就再对那个事件走一次。\n\n返回：链上每步的边名/理由、dead ends（走到没有因果边的地方）。这是给反思者读的叙事，不是机器判因果。', inputSchema: { type: 'object', properties: { event_id: { type: 'number', description: '链的起点事件 id' }, depth: { type: 'number', description: '最多走几层, 默认 4, 最大 8' }, direction: { type: 'string', enum: ['upstream', 'downstream', 'both'], description: '默认 both' } }, required: ['event_id'] } },
];

// ━━━ MCP 工具分发（与云端行为一致；兼容旧本地参数别名 uuid/id）━━━
async function callTool(name, a) {
  // ── profile（档案卡：助手的自我叙事+关系认知，按 id 顺序拼起来即下一轮 system prompt）──
  if (name === 'archive_profile_read') {
    const mode = String(a.mode ?? 'cards');
    const items = dbListProfile();
    if (mode === 'assembled') {
      return { ok: true, mode, count: items.length, prompt: dbAssembleProfile(), note: '按 id 顺序拼接——这就是助手下一轮对话的系统提示词全文' };
    }
    return { ok: true, mode: 'cards', count: items.length, items, note: '按 id 排列——拼起来即助手下一轮对话的系统提示词; mode=assembled 直接看拼接全文' };
  }
  if (name === 'archive_profile_edit') {
    const hasTarget = a.id != null || (a.title != null && String(a.title).trim() !== '');
    if (!hasTarget) return { ok: false, error: '需要 id 或 title 定位一张卡(都传时以 id 为准)' };
    const target = a.id != null ? { id: Number(a.id) } : { title: String(a.title).trim() };
    const wantContent = a.content != null && String(a.content).trim() !== '';
    const newTitle = a.new_title != null ? String(a.new_title).trim() : null;
    const wantMove = a.new_id != null;
    if (!wantContent && !newTitle && !wantMove) {
      return { ok: false, error: '没有要改的东西——content(改内容) / new_title(改卡名) / new_id(改序号) 至少传一个' };
    }
    let card = dbGetProfile(target);
    let created = false;
    if (!card) {
      // 建档只发生在: 按 title 定位 + 不存在 + 只改内容
      if (target.id != null || newTitle || wantMove) return { ok: false, error: `没有「${target.id ?? target.title}」这张卡(改名/移动要求卡已存在; 新建用 title+content)` };
      const r = dbUpsertProfile(target.title, String(a.content));
      card = r.card;
      created = true;
    }
    if (wantContent && !created) {
      DB.prepare('UPDATE profile SET content = ?, updated_at = ? WHERE title = ?').run(String(a.content), ts(), card.title);
    }
    if (newTitle) {
      const r = dbRenameProfile({ title: card.title }, newTitle);
      if (r.error) return { ok: false, error: r.error };
      card = r.card;
    }
    if (wantMove) {
      const r = dbMoveProfile({ title: card.title }, Number(a.new_id));
      if (r.error) return { ok: false, error: r.error };
      card = r.card;
    }
    card = dbGetProfile({ title: card.title });
    const notes = [];
    if (created) notes.push('新卡已建档(排在最后)');
    else if (wantContent) notes.push('内容已整卡覆盖');
    if (newTitle) notes.push(`卡名已改为「${card.title}」`);
    if (wantMove) notes.push(`已移到 #${card.id}, 其余卡自动重排`);
    return {
      ok: true, created,
      card: { id: card.id, title: card.title, content_len: card.content.length, updated_at: card.updated_at },
      note: notes.join('; ') || '无改动',
    };
  }
  // ── night（夜间唤醒开关；查/切平权——REST 与 MCP 双通道都能看都能关都能开）──
  if (name === 'archive_night_status') {
    const sw = dbNightSwitch();
    const last = DB.prepare('SELECT * FROM night_reports ORDER BY date DESC LIMIT 1').get();
    return { ok: true, enabled: sw.enabled, updated_at: sw.updated_at, last_report: last ? dbNightReportSummary(last) : null };
  }
  if (name === 'archive_night_toggle') {
    if (typeof a.enabled !== 'boolean') return { ok: false, error: '需要 enabled: true / false' };
    const sw = dbNightSetEnabled(a.enabled);
    return {
      ok: true, ...sw,
      note: a.enabled
        ? '夜间唤醒已打开——今晚 04:00 助手会醒来整理记忆，晨报会推给用户'
        : '夜间唤醒已关闭——助手今晚不会被唤醒，整晚安睡',
    };
  }
  if (name === 'archive_night_reports') {
    const limit = clampInt(a.limit, 3, 1, 20);
    const rows = DB.prepare('SELECT * FROM night_reports ORDER BY date DESC LIMIT ?').all(limit);
    const reports = rows.map((row) => {
      let detail = {};
      try { detail = JSON.parse(row.detail_json || '{}'); } catch { /* detail 损坏不影响摘要 */ }
      return { ...dbNightReportSummary(row), dream_words: detail.dream_words ?? null, notes: detail.notes ?? [] };
    });
    return {
      ok: true, count: reports.length, reports,
      note: '单晚逐条判定明细（verdict 理由原文/改名/时间线）走 REST /archive/night-reports/:date; 开关用 archive_night_toggle',
    };
  }
  // ── events ──
  if (name === 'archive_list_events') {
    const { items, total } = dbListEvents({ theme: a.theme, event_type: a.event_type, time_start: a.time_start, time_end: a.time_end, sort: a.sort, limit: a.limit, offset: a.offset });
    const slim = items.map((it) => {
      if (a.include_summary) return it;
      const { summary, ...rest } = it;
      return rest;
    });
    return { ok: true, total, items: slim };
  }
  if (name === 'archive_search_events') {
    const query = String(a.query ?? '').trim();
    if (!query) return { ok: false, error: 'query is required' };
    return await archiveSearchEvents({ query, k: a.k });
  }
  if (name === 'archive_read_event') {
    const eventId = Number(a.event_id ?? a.id);
    if (!Number.isFinite(eventId) || eventId <= 0) return { ok: false, error: 'event_id required' };
    const event = dbGetEvent(eventId);
    if (!event) return { ok: false, error: `event ${eventId} not found` };
    return { ok: true, event };
  }
  if (name === 'archive_create_event') {
    const title = String(a.title ?? '').trim();
    if (!title) return { ok: false, error: 'title is required' };
    const event = dbCreateEvent(
      title,
      a.summary ? String(a.summary).trim() || null : null,
      String(a.time_span_start ?? a.time_start ?? ''),
      a.time_span_end ?? a.time_end ?? null,
      Array.isArray(a.themes) ? a.themes.map(String) : null,
      Array.isArray(a.source_block_ids) ? a.source_block_ids.map(Number).filter((n) => Number.isFinite(n)) : null,
      typeof a.importance === 'number' ? Math.max(0, Math.min(1, a.importance)) : 0.5,
      ['moment', 'period', 'milestone', 'ritual'].includes(String(a.event_type ?? '')) ? String(a.event_type) : 'moment',
    );
    dbMarkEventDirty(event.id);
    return { ok: true, event, note: '向量待重建(rebuild 会嵌入建边)' };
  }
  if (name === 'archive_update_event') {
    const eventId = Number(a.event_id ?? a.id);
    if (!Number.isFinite(eventId) || eventId <= 0) return { ok: false, error: 'event_id required' };
    const updated = dbUpdateEvent(eventId, {
      title: a.title !== undefined ? String(a.title).trim() : undefined,
      summary: a.summary !== undefined ? (String(a.summary).trim() || null) : undefined,
      time_span_start: a.time_span_start ?? a.time_start ?? undefined,
      time_span_end: a.time_span_end ?? a.time_end,
      themes: a.themes !== undefined ? (Array.isArray(a.themes) ? a.themes.map(String) : null) : undefined,
      source_block_ids: a.source_block_ids !== undefined ? (Array.isArray(a.source_block_ids) ? a.source_block_ids.map(Number).filter((n) => Number.isFinite(n)) : null) : undefined,
      importance: a.importance !== undefined ? Math.max(0, Math.min(1, Number(a.importance))) : undefined,
      event_type: a.event_type !== undefined ? (['moment', 'period', 'milestone', 'ritual'].includes(String(a.event_type)) ? String(a.event_type) : undefined) : undefined,
    });
    if (!updated) return { ok: false, error: 'event not found or no changes' };
    const event = dbGetEvent(eventId);
    dbMarkEventDirty(eventId);
    return { ok: true, event, note: '已改, 向量标脏(rebuild 会重嵌入重建边)' };
  }
  if (name === 'archive_delete_event') {
    const eventId = Number(a.event_id ?? a.id);
    if (!Number.isFinite(eventId) || eventId <= 0) return { ok: false, error: 'event_id required' };
    const ev = dbGetEvent(eventId);
    if (!ev) return { ok: false, error: `event ${eventId} not found` };
    dbDeleteEvent(eventId);
    DB.prepare('DELETE FROM event_edges WHERE event_a = ? OR event_b = ?').run(eventId, eventId);
    deleteEventVectors(eventId);
    return { ok: true, deleted: true, id: eventId };
  }
  // ── 事件网络 ──
  if (name === 'archive_event_link') {
    const r = createManualLink(a);
    if (r.error) return { ok: false, error: r.error };
    return { ok: true, edge: r.edge, note: '手动边, rebuild 会保留' };
  }
  if (name === 'archive_event_unlink') {
    const a1 = Number(a.event_a);
    const b1 = Number(a.event_b);
    const type = String(a.edge_type ?? '');
    if (!Number.isFinite(a1) || !Number.isFinite(b1) || a1 <= 0 || b1 <= 0 || a1 === b1) return { ok: false, error: '需要两个不同的正整数 event id' };
    if (type !== 'causal' && type !== 'overlap' && type !== 'thematic') return { ok: false, error: 'edge_type 必须是 causal / overlap / thematic' };
    const removed = deleteManualLink(a1, b1, type);
    return { ok: removed > 0, removed, note: removed > 0 ? '已解除手动边' : '没有找到对应的手动边（自动边不受影响）' };
  }
  if (name === 'archive_overview') {
    const evs = DB.prepare('SELECT COUNT(*) AS c FROM events').get();
    const comms = DB.prepare('SELECT COUNT(*) AS c FROM event_communities').get();
    const edges = DB.prepare('SELECT COUNT(*) AS c FROM event_edges').get();
    const recentEvents = DB.prepare('SELECT id, title, event_type, time_span_start FROM events ORDER BY updated_at DESC LIMIT 5').all();
    return {
      ok: true,
      counts: { events: evs?.c ?? 0, communities: comms?.c ?? 0, edges: edges?.c ?? 0 },
      recent_events: recentEvents,
      note: '深入: archive_list_events / archive_event_subgraph / archive_associate',
    };
  }
  if (name === 'archive_list_communities') {
    const limit = Math.min(Math.max(Number(a.limit ?? 50), 1), 100);
    const results = DB.prepare('SELECT * FROM event_communities ORDER BY internal_edges DESC, id ASC LIMIT ?').all(limit);
    const ids = new Set();
    for (const r of results) for (const m of JSON.parse(r.member_ids)) ids.add(m);
    const titleMap = dbEventTitleMap([...ids]);
    return {
      ok: true, total: results.length,
      communities: results.map((r) => {
        const members = JSON.parse(r.member_ids);
        const coreId = r.core_event_id;
        return {
          id: r.id, name: r.name, renamed: r.renamed === 1, note: r.note,
          member_titles: members.map((m) => titleMap.get(m) ?? `#${m}`),
          core_event: { id: coreId, title: titleMap.get(coreId) ?? `#${coreId}` },
          internal_edges: r.internal_edges, time_span_start: r.time_span_start, time_span_end: r.time_span_end,
        };
      }),
      note: 'rebuild 自动重算; 命名过的按成员保留名字',
    };
  }
  if (name === 'archive_community_rename') {
    const cid = Number(a.community_id);
    if (!Number.isFinite(cid) || cid <= 0) return { ok: false, error: 'community_id required' };
    const name = String(a.name ?? '').trim();
    const note = a.note !== undefined ? String(a.note) : null;
    const r = DB.prepare('UPDATE event_communities SET name = ?, note = ?, renamed = ?, updated_at = ? WHERE id = ?')
      .run(name, note, name ? 1 : 0, ts(), cid);
    if (r.changes === 0) return { ok: false, error: 'community not found' };
    return { ok: true, id: cid, name, note, note2: name ? '已命名, rebuild 会保留' : '已回到未命名' };
  }
  if (name === 'archive_community_delete') {
    const cid = Number(a.community_id);
    if (!Number.isFinite(cid) || cid <= 0) return { ok: false, error: 'community_id required' };
    const removed = DB.prepare('DELETE FROM event_communities WHERE id = ?').run(cid).changes;
    return { ok: removed > 0, removed, note: removed > 0 ? '已删除(下次 rebuild 会重新聚出)' : 'community not found' };
  }
  if (name === 'archive_event_graph') {
    const eventId = Number(a.event_id ?? a.id);
    if (!Number.isFinite(eventId) || eventId <= 0) return { ok: false, error: 'event_id required' };
    const result = getEventGraph(eventId);
    if (!result) return { ok: false, error: `event ${eventId} not found` };
    return { ok: true, ...result, note: 'thematic 边优先于 overlap 边(隔久仍相似才是本质关联); 边没建就 archive_rebuild_network' };
  }
  if (name === 'archive_event_subgraph') {
    const seedList = [];
    if (a.seed != null && a.seed !== '') seedList.push(Number(a.seed));
    if (Array.isArray(a.seeds)) for (const s of a.seeds) seedList.push(Number(s));
    const seeds = [...new Set(seedList)].filter((n) => Number.isFinite(n) && n > 0);
    if (seeds.length === 0) return { ok: false, error: '需要 seed 或 seeds(正整数 event id)' };
    if (seeds.length > 10) return { ok: false, error: 'seeds 一次最多 10 个' };
    const r = buildSubgraph({ seeds, radius: a.radius, max_nodes: a.max_nodes, edge_types: a.edge_types, include_summaries: a.include_summaries === true });
    if (r.error) return { ok: false, error: r.error };
    return r;
  }
  if (name === 'archive_network_stats') {
    const s = dbEventNetworkStats();
    return {
      ok: true,
      total_events: s.total_events, total_edges: s.total_edges, overlap: s.overlap, thematic: s.thematic,
      top_connected: s.per_event.slice(0, 10),
      bridges: s.bridges,
      cut_vertices: s.cut_vertices,
      note: 'bridges=跨故事群的边(群与群的连接组织); cut_vertices=拿掉就会断网的枢纽事件; 全量 per_event 明细走 REST /archive/events/network-stats',
    };
  }
  if (name === 'archive_associate') {
    try {
      return await archiveAssociate(a);
    } catch (e) {
      return { ok: false, error: `associate failed: ${e.message}` };
    }
  }
  if (name === 'archive_event_path') {
    try {
      return archiveEventPath(a);
    } catch (e) {
      return { ok: false, error: `event_path failed: ${e.message}` };
    }
  }
  if (name === 'archive_rebuild_network') {
    const r = await rebuildEventNetwork();
    return { ok: true, ...r, dirty_remaining: r.embed_failures.length };
  }
  if (name === 'archive_restore_network') {
    const r = restoreEventNetwork();
    if (!r) return { ok: false, error: '没有可恢复的备份(rebuild 成功时备份已删除)' };
    return { ok: true, ...r, note: '已从备份恢复 rebuild 前的网络' };
  }
  if (name === 'archive_causal_probe') {
    if (String(a.mode ?? '') === 'remote') {
      try {
        return remoteCausalCandidates(a);
      } catch (e) {
        return { ok: false, error: `remote probe failed: ${e.message}` };
      }
    }
    const eventId = Number(a.event_id);
    if (!Number.isFinite(eventId) || eventId <= 0) return { ok: false, error: 'event_id required' };
    const mode = a.mode === 'cause' || a.mode === 'effect' ? a.mode : 'both';
    const limit = Math.max(1, Math.min(Number(a.limit) || PROBE_DEFAULT_LIMIT, PROBE_MAX_LIMIT));
    const detail = a.detail === true;
    const g = loadProbeGraph();
    const y = g.evs.get(eventId);
    if (!y) return { ok: false, error: `event ${eventId} not found` };
    const pairKey = (x, z) => `${Math.min(x, z)}:${Math.max(x, z)}`;
    const excludedPairs = new Set();
    for (const e of g.byEvent.get(eventId) ?? []) {
      if (e.edge_type === 'causal') excludedPairs.add(pairKey(eventId, e.a === eventId ? e.b : e.a));
    }
    for (const rv of dbListReviews()) {
      if (rv.candidate_id == null) continue;
      if ((rv.verdict === 'reject' || rv.verdict === 'link') && (rv.event_id === eventId || rv.candidate_id === eventId)) {
        excludedPairs.add(pairKey(rv.event_id, rv.candidate_id));
      }
    }
    const yT = tsMs(y.time_span_start);
    const linkedCauses = [];
    const linkedEffects = [];
    for (const e of g.byEvent.get(eventId) ?? []) {
      if (e.edge_type !== 'causal') continue;
      const xId = e.a === eventId ? e.b : e.a;
      const x = g.evs.get(xId);
      if (!x) continue;
      const xT = tsMs(x.time_span_start);
      const isCause = e.dir === 'a→b' ? e.b === eventId : e.dir === 'b→a' ? e.a === eventId
        : (yT !== null && xT !== null ? xT < yT : true);
      const rec = { event_id: xId, title: x.title, ...edgeReasonOf(e.detail) };
      (isCause ? linkedCauses : linkedEffects).push(rec);
    }
    const candTo = (c) => ({
      event_id: c.event.id, title: c.event.title, event_type: c.event.event_type, summary: summaryClip(c.event.summary, detail ? 400 : 120),
      time_span_start: c.event.time_span_start, community_id: c.event.community_id,
      score: Math.round(c.score * 100) / 100, hop: c.hop, kind: c.kind, paths: c.paths,
      ...(c.evidence ? { evidence: c.evidence } : {}),
    });
    const out = {
      ok: true,
      center: { event_id: y.id, title: y.title, event_type: y.event_type, time_span_start: y.time_span_start, time_span_end: y.time_span_end, community_id: y.community_id, degree: y.degree },
      linked_causes: linkedCauses,
      linked_effects: linkedEffects,
    };
    if (mode === 'cause' || mode === 'both') out.candidate_causes = probeCandidates(g, y, 'cause', excludedPairs, limit).map(candTo);
    if (mode === 'effect' || mode === 'both') out.candidate_effects = probeCandidates(g, y, 'effect', excludedPairs, limit).map(candTo);
    out.note = '候选只是机器出的题: 判定用 archive_causal_review(link/reject/done/defer)。hop1=已有边提升, hop2=两跳链, hop3=同社区弱线索。';
    return out;
  }
  if (name === 'archive_causal_review') {
    const present = a.present === true;
    if (present && (a.verdict || a.verdicts)) return { ok: false, error: 'present 模式只端菜不判定——要判定请去掉 present 再调一次' };
    if (present) {
      const focus = ['causes', 'effects', 'auto'].includes(String(a.focus ?? '')) ? String(a.focus) : 'causes';
      const detail = a.detail === true;
      const g = loadProbeGraph();
      const reviews = dbListReviews();
      const doneIds = new Set();
      const deferUntil = new Map();
      for (const rv of reviews) {
        if (rv.verdict === 'done') doneIds.add(rv.event_id);
        if (rv.verdict === 'defer' && rv.defer_until) deferUntil.set(rv.event_id, rv.defer_until);
      }
      const nowMsNow = Date.now();
      const missingIn = [];    // 无因: 没有 causal 边指向它
      const missingOut = [];   // 无果: 它不导向任何 causal 边(叙事死胡同)
      for (const ev of g.evs.values()) {
        if (doneIds.has(ev.id)) continue;
        const du = deferUntil.get(ev.id);
        if (du && Date.parse(du) > nowMsNow) continue;
        const causalEdges = (g.byEvent.get(ev.id) ?? []).filter((e) => e.edge_type === 'causal');
        const hasIn = causalEdges.some((e) => {
          if (e.dir === 'a→b') return e.b === ev.id;
          if (e.dir === 'b→a') return e.a === ev.id;
          return true;
        });
        const hasOut = causalEdges.some((e) => {
          if (e.dir === 'a→b') return e.a === ev.id;
          if (e.dir === 'b→a') return e.b === ev.id;
          return true;
        });
        if (!hasIn) missingIn.push(ev);
        if (!hasOut) missingOut.push(ev);
      }
      let unexplained;
      if (focus === 'effects') unexplained = missingOut;
      else if (focus === 'auto') {
        const inIds = new Set(missingIn.map((e) => e.id));
        unexplained = [...missingIn, ...missingOut.filter((e) => !inIds.has(e.id))];
      } else unexplained = missingIn;
      unexplained.sort((p, q) => q.importance - p.importance || p.degree - q.degree || p.id - q.id);
      const pairKey = (x, z) => `${Math.min(x, z)}:${Math.max(x, z)}`;
      const excludedPairs = new Set();
      for (const rv of reviews) {
        if (rv.candidate_id == null) continue;
        if (rv.verdict === 'reject' || rv.verdict === 'link') excludedPairs.add(pairKey(rv.event_id, rv.candidate_id));
      }
      const topEvents = unexplained.slice(0, 3);
      const presented = [];
      for (const ev of topEvents) {
        const causes = probeCandidates(g, ev, 'cause', excludedPairs, 2);
        const effects = probeCandidates(g, ev, 'effect', excludedPairs, 2);
        let causeList = causes.slice(0, 1);
        let effectList = effects.slice(0, 1);
        if (causeList.length === 0) effectList = effects.slice(0, 2);
        if (effectList.length === 0) causeList = causes.slice(0, 2);
        const toItem = (c) => ({ event_id: c.event.id, title: c.event.title, summary: summaryClip(c.event.summary, detail ? 400 : 120), score: Math.round(c.score * 100) / 100, kind: c.kind, paths: c.paths, ...(c.evidence ? { evidence: c.evidence } : {}) });
        presented.push({
          event_id: ev.id, title: ev.title, event_type: ev.event_type, time_span_start: ev.time_span_start,
          importance: ev.importance, degree: ev.degree, community_id: ev.community_id,
          candidate_causes: causeList.map(toItem),
          candidate_effects: effectList.map(toItem),
        });
      }
      return {
        ok: true, focus,
        queue_sizes: { causes: missingIn.length, effects: missingOut.length },
        unexplained_count: unexplained.length, presented_count: presented.length, items: presented,
        note: 'judge: link {event_id, candidate_id, reason} / reject {event_id, candidate_id} / done {event_id} / defer {event_id, defer_days}, 或 verdicts:[...] 批量判。focus: causes=无因(默认) / effects=无果死胡同 / auto=并集。detail=true 摘要放宽到 400 字。',
      };
    }
    // ── 判定: 批量 verdicts(一次事务) 或单条(兼容旧参数) ──
    if (a.verdicts != null) {
      if (a.verdict != null) return { ok: false, error: 'verdicts(批量) 与 verdict(单条) 不能同时传' };
      if (!Array.isArray(a.verdicts) || a.verdicts.length === 0) return { ok: false, error: 'verdicts 必须是非空数组' };
      if (a.verdicts.length > 50) return { ok: false, error: '一次最多判 50 条' };
      const results = [];
      const counts = { link: 0, reject: 0, done: 0, defer: 0, failed: 0 };
      DB.exec('BEGIN');
      try {
        for (const item of a.verdicts) {
          const r = (item && typeof item === 'object') ? applyOneVerdict(item, a.decided_by) : { ok: false, error: 'verdicts[] 元素必须是对象' };
          results.push(r);
          if (r.ok) counts[r.verdict]++;
          else counts.failed++;
        }
        DB.exec('COMMIT');
      } catch (e) {
        DB.exec('ROLLBACK');
        return { ok: false, error: `批量判定失败(已全部回滚): ${e.message}` };
      }
      return { ok: counts.failed === 0, counts, results, note: '批量判定完成; link 各项带 echo(新边两侧一跳因果邻域)——看看判定让链长成了什么样' };
    }
    return applyOneVerdict(a);
  }
  if (name === 'archive_causal_chain') {
    const eventId = Number(a.event_id);
    if (!Number.isFinite(eventId) || eventId <= 0) return { ok: false, error: 'event_id required' };
    const depth = Math.max(1, Math.min(Number(a.depth) || 4, 8));
    const dir = a.direction === 'upstream' || a.direction === 'downstream' ? a.direction : 'both';
    const g = loadProbeGraph();
    const start = g.evs.get(eventId);
    if (!start) return { ok: false, error: `event ${eventId} not found` };
    const walk = (mode) => {
      const steps = [];
      const visited = new Set([start.id]);
      let cur = start;
      let deadEnd = false;
      for (let i = 0; i < depth; i++) {
        const curT = tsMs(cur.time_span_start);
        const nexts = [];
        for (const e of g.byEvent.get(cur.id) ?? []) {
          if (e.edge_type !== 'causal' || visited.has(e.a === cur.id ? e.b : e.a)) continue;
          const xId = e.a === cur.id ? e.b : e.a;
          const goes = e.dir === 'a→b' ? (e.b === cur.id ? 'up' : 'down')
            : e.dir === 'b→a' ? (e.a === cur.id ? 'up' : 'down')
            : 'both';
          if (mode === 'upstream' && goes === 'down') continue;
          if (mode === 'downstream' && goes === 'up') continue;
          const x = g.evs.get(xId);
          if (!x) continue;
          nexts.push({ ev: x, edge: e, ...edgeReasonOf(e.detail) });
        }
        if (nexts.length === 0) { deadEnd = true; break; }
        nexts.sort((p, q) => {
          const wp = q.edge.weight - p.edge.weight;
          if (wp !== 0) return wp;
          const dt = (tsMs(p.ev.time_span_start) ?? Infinity) - curT;
          const dt2 = (tsMs(q.ev.time_span_start) ?? Infinity) - curT;
          return Math.abs(dt) - Math.abs(dt2) || p.ev.id - q.ev.id;
        });
        const main = nexts[0];
        steps.push({
          event_id: main.ev.id, title: main.ev.title,
          via: `${main.edge.edge_type}(${main.edge.dir === 'mutual' ? '双向' : main.edge.dir})`,
          reason: main.reason ?? main.name ?? null,
          branches: nexts.slice(1).map((b) => ({ event_id: b.ev.id, title: b.ev.title, reason: b.reason ?? b.name ?? null })),
        });
        visited.add(main.ev.id);
        cur = main.ev;
      }
      return { steps, dead_end: deadEnd, ended_at: { event_id: cur.id, title: cur.title } };
    };
    const out = { ok: true, start: { event_id: start.id, title: start.title } };
    if (dir === 'upstream' || dir === 'both') out.upstream = walk('upstream');
    if (dir === 'downstream' || dir === 'both') out.downstream = walk('downstream');
    out.note = '每层只展开权重最高的那条因果边, 分支在 branches 里(想深挖就对那个事件再走一次)。';
    return out;
  }
  return { ok: false, error: `unknown tool: ${name}` };
}

// ━━━ viz 实时聚合（云端 viz.ts vizData 移植，2026-08-23 搬进主家）━━━
const VIZ_PALETTE = ['#b07f86', '#7fa3b0', '#8b9e6b', '#c9a86a', '#9b8bb5', '#6b9e8f', '#c08a6a',
  '#7a8bb5', '#b58bb5', '#8a9b6b', '#c96a6a', '#5f9ea0', '#cd7f8a', '#a0a0c8', '#c8a06a',
  '#b8b0a6', '#d8d2c8'];
const VIZ_TYPE_LABEL = { moment: '瞬间', period: '时期', milestone: '里程碑', ritual: '仪式' };
const VIZ_EDGE_STYLE = {
  overlap: { color: '#c9c2b8', label: '同时段' },
  thematic: { color: '#7fa3b0', label: '语义血缘' },
  causal: { color: '#c9a86a', label: '因果关联' },
};
const VIZ_PRIO = { causal: 2, thematic: 1, overlap: 0 };

function vizData() {
  const events = DB.prepare('SELECT id, title, summary, time_span_start, time_span_end, importance, event_type FROM events').all();
  const edges = DB.prepare('SELECT event_a, event_b, edge_type, detail FROM event_edges').all();
  const comms = DB.prepare('SELECT id, name, member_ids FROM event_communities').all();

  // 社区映射（插入序 = 聚类结果序）
  const commOf = new Map();
  const commName = new Map();
  for (const c of comms) {
    commName.set(c.id, c.name ? c.name : `未命名·社区${c.id}`);
    for (const mid of JSON.parse(c.member_ids)) commOf.set(mid, c.id);
  }
  // 无边事件 → 孤立; 有边未聚类 → 未聚类
  const titleOf = new Map();
  for (const e of events) titleOf.set(e.id, e.title);
  const edgeNodes = new Set();
  for (const e of edges) { edgeNodes.add(e.event_a); edgeNodes.add(e.event_b); }
  let nextCid = 1000;
  let isoCid = null, unCid = null;
  for (const e of events) {
    if (commOf.has(e.id)) continue;
    if (!edgeNodes.has(e.id)) {
      if (isoCid === null) { isoCid = nextCid++; commName.set(isoCid, '孤立'); }
      commOf.set(e.id, isoCid);
    } else {
      if (unCid === null) { unCid = nextCid++; commName.set(unCid, '未聚类'); }
      commOf.set(e.id, unCid);
    }
  }

  const commColor = new Map();
  let ci = 0;
  for (const cid of commName.keys()) commColor.set(cid, VIZ_PALETTE[ci++ % VIZ_PALETTE.length]);
  const commSize = new Map();
  for (const cid of commOf.values()) commSize.set(cid, (commSize.get(cid) ?? 0) + 1);
  const commIdsSorted = [...commName.keys()].sort((a, b) => (commSize.get(b) ?? 0) - (commSize.get(a) ?? 0));
  const catIdx = new Map();
  commIdsSorted.forEach((cid, idx) => catIdx.set(cid, idx));

  const deg = new Map();
  for (const e of edges) {
    deg.set(e.event_a, (deg.get(e.event_a) ?? 0) + 1);
    deg.set(e.event_b, (deg.get(e.event_b) ?? 0) + 1);
  }

  const nodes = [];
  for (const e of events) {
    const cid = commOf.get(e.id);
    const d = deg.get(e.id) ?? 0;
    nodes.push({
      id: e.id,
      name: e.title,
      value: d,
      symbolSize: 10 + Math.min(d * 0.9, 10),
      category: catIdx.get(cid),
      itemStyle: { color: commColor.get(cid) },
      event_type: VIZ_TYPE_LABEL[e.event_type] ?? e.event_type,
      event_kind: e.event_type,
      time: e.time_span_start ?? '',
      time_end: e.time_span_end ?? '',
      summary: (e.summary ?? '').slice(0, 120),
      importance: e.importance,
      comm_name: commName.get(cid),
    });
  }

  // 边合并（同 pair 多类型 → 主类型排序 + 标签合并 + causal 带名字/理由）
  const pairs = new Map();
  for (const e of edges) {
    const a = Math.min(e.event_a, e.event_b), b = Math.max(e.event_a, e.event_b);
    const key = `${a}:${b}`;
    if (!pairs.has(key)) pairs.set(key, { types: [], causalDetail: null, semanticScore: null });
    const pair = pairs.get(key);
    pair.types.push(e.edge_type);
    if (e.edge_type === 'causal' && e.detail) pair.causalDetail = e.detail;
    if (e.edge_type === 'thematic' && e.detail) {
      try {
        const detail = JSON.parse(e.detail);
        const score = Number(detail.score ?? detail.similarity ?? detail.semantic_score);
        if (Number.isFinite(score)) pair.semanticScore = Math.max(pair.semanticScore ?? -Infinity, score);
      } catch { /* 旧数据没有可读取的语义分数 */ }
    }
  }
  // 结构化因果关系：没有明确方向的数据保留为无向线，不能以日期臆测因果方向
  const causal = [];
  const links = [];
  for (const [key, info] of pairs) {
    const [a, b] = key.split(':').map(Number);
    const types = [...new Set(info.types)].sort((x, y) => (VIZ_PRIO[y] ?? 0) - (VIZ_PRIO[x] ?? 0));
    const main = types[0];
    const st = VIZ_EDGE_STYLE[main] ?? VIZ_EDGE_STYLE.overlap;
    const labels = types.map((t) => (VIZ_EDGE_STYLE[t] ?? VIZ_EDGE_STYLE.overlap).label).join(' + ');
    let extra = '';
    if (types.includes('causal')) {
      let from = a, to = b, directed = false;
      let name;
      let reason;
      try {
        const dt = info.causalDetail ? JSON.parse(info.causalDetail) : {};
        if (dt.name) { name = String(dt.name); extra = `「${name}」`; }
        if (dt.reason) { reason = String(dt.reason).slice(0, 80); extra += ' ' + reason; }
        if (dt.dir === 'a→b') {
          extra += ` · ${titleOf.get(a) ?? a} → ${titleOf.get(b) ?? b}`;
          directed = true;
        } else if (dt.dir === 'b→a') {
          extra += ` · ${titleOf.get(b) ?? b} → ${titleOf.get(a) ?? a}`;
          from = b; to = a; directed = true;
        }
      } catch { /* detail 不是 JSON → 忽略 */ }
      causal.push({ from, to, directed, ...(name ? { name } : {}), ...(reason ? { reason } : {}) });
    }
    links.push({
      source: a, target: b,
      lineStyle: { color: st.color, width: 1 + types.length * 0.6, type: main === 'causal' ? 'dashed' : 'solid' },
      types: labels,
      extra,
      ...(info.semanticScore !== null ? { semantic_score: info.semanticScore } : {}),
    });
  }

  const categories = commIdsSorted.map((cid) => ({
    name: commName.get(cid),
    itemStyle: { color: commColor.get(cid) },
  }));

  return {
    nodes, links, categories, causal,
    stats: { events: events.length, edges: edges.length, communities: comms.length },
  };
}

// ━━━ REST（全部路径与云端一致）━━━
async function handleRest(ctx) {
  const { url, subpath, method, body, isAny, isAdmin } = ctx;
  const json = (data, status = 200) => ({ status, json: data });
  const err = (msg, status = 400) => json({ ok: false, error: msg }, status);

  // ── viz（2026-08-23 从云端 events.example.com 搬进主家，挂 /archive/viz；页面 fetch 已改相对路径）──
  const VIZ_COOKIE = 'viz_token';
  const hasVizCookie = () => {
    const ck = String(ctx.req.headers.cookie ?? '');
    const m = ck.match(new RegExp(`(?:^|; )${VIZ_COOKIE}=([^;]*)`));
    return !!m && m[1] === CFG.adminToken;
  };
  if (subpath === '/echarts.min.js' && method === 'GET') {
    // echarts 5.6.0 本地副本（2026-08-24 从 jsdelivr 收进服务器，viz 不再依赖 CDN）
    return { __stream: (res) => {
      res.writeHead(200, { 'Content-Type': 'application/javascript; charset=utf-8', 'Cache-Control': 'public, max-age=86400' });
      res.end(fs.readFileSync(path.join(__dirname, 'echarts.min.js')));
    } };
  }
  if (subpath === '/viz' || subpath === '/viz/') {
    if (method !== 'GET') return err('method not allowed', 405);
    const html = fs.readFileSync(path.join(__dirname, 'viz.html'), 'utf8');
    if (url.searchParams.get('token') === CFG.adminToken) {
      return { __stream: (res) => {
        res.writeHead(200, {
          'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store',
          'Set-Cookie': `${VIZ_COOKIE}=${CFG.adminToken}; Path=/archive; Max-Age=2592000; HttpOnly; SameSite=Lax`,
        });
        res.end(html);
      } };
    }
    if (hasVizCookie()) {
      return { __stream: (res) => {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end(html);
      } };
    }
    return { __stream: (res) => {
      res.writeHead(401, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('记忆网络需要访问口令: events.example.com/archive/viz?token=xxx');
    } };
  }
  if (subpath === '/data' && method === 'GET') {
    if (!hasVizCookie()) return err('unauthorized', 401);
    return json(await vizData());
  }
  if (subpath === '/link' && (method === 'POST' || method === 'DELETE')) {
    if (!hasVizCookie()) return err('unauthorized', 401);
    if (method === 'POST') {
      const b = JSON.parse(body || 'null');
      if (!b || typeof b !== 'object') return err('invalid json body', 400);
      const r = createManualLink(b);
      if (r.error) return err(r.error, r.status ?? 400);
      return json({ ok: true, edge: r.edge });
    }
    const a = Number(url.searchParams.get('event_a') ?? 0);
    const b = Number(url.searchParams.get('event_b') ?? 0);
    const type = String(url.searchParams.get('edge_type') ?? '');
    if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0 || a === b) return err('需要两个不同的正整数 event id');
    if (type !== 'causal' && type !== 'overlap' && type !== 'thematic') return err('edge_type 必须是 causal / overlap / thematic');
    const removed = deleteManualLink(a, b, type);
    return json({ ok: true, removed });
  }
  if (subpath === '/communities/rebuild' && method === 'POST') {
    if (!hasVizCookie()) return err('unauthorized', 401);
    const r = runCommunityDetection();
    return json({ ok: true, communities: r.communities, preserved: r.preserved });
  }

  // ── admin only ──

  // 以下 REST: ADMIN + APP 平权
  if (!isAny(ctx.auth)) return err('unauthorized', 401);

  if (subpath === '/stats' || subpath === '/stats/') {
    if (method !== 'GET') return err('method not allowed', 405);
    return json({ ok: true, ...dbStats() });
  }
  // ── events ──
  if (subpath === '/events' || subpath === '/events/') {
    if (method !== 'GET') return err('method not allowed', 405);
    const { items, total } = dbListEvents({
      q: url.searchParams.get('q')?.trim() || undefined,
      theme: url.searchParams.get('theme')?.trim() || undefined,
      event_type: url.searchParams.get('event_type')?.trim() || undefined,
      time_start: url.searchParams.get('time_start')?.trim() || undefined,
      time_end: url.searchParams.get('time_end')?.trim() || undefined,
      sort: url.searchParams.get('sort')?.trim() || undefined,
      limit: clampInt(url.searchParams.get('limit'), 50, -1, 200),
      offset: clampInt(url.searchParams.get('offset'), 0, 0, Number.MAX_SAFE_INTEGER),
    });
    return json({ ok: true, items, total });
  }
  if (subpath === '/events/create' || subpath === '/events/create/') {
    if (method !== 'POST') return err('method not allowed', 405);
    const b = JSON.parse(body || 'null');
    if (!b || typeof b !== 'object') return err('invalid json body', 400);
    const title = String(b.title ?? '').trim();
    if (!title) return err('title is required', 400);
    const event = dbCreateEvent(
      title,
      b.summary ? String(b.summary).trim() || null : null,
      String(b.time_span_start ?? b.time_start ?? ''),
      b.time_span_end ?? b.time_end ?? null,
      Array.isArray(b.themes) ? b.themes.map(String) : null,
      Array.isArray(b.source_block_ids) ? b.source_block_ids.map(Number).filter((n) => Number.isFinite(n)) : null,
      typeof b.importance === 'number' ? Math.max(0, Math.min(1, b.importance)) : 0.5,
      ['moment', 'period', 'milestone', 'ritual'].includes(String(b.event_type ?? '')) ? String(b.event_type) : 'moment',
    );
    dbMarkEventDirty(event.id);
    return json({ ok: true, event }, 201);
  }
  const eventsMatch = subpath.match(/^\/events\/(\d+)\/?$/);
  if (eventsMatch) {
    const eventId = parseInt(eventsMatch[1], 10);
    if (method === 'GET') {
      const event = dbGetEvent(eventId);
      if (!event) return err('event not found', 404);
      return json({ ok: true, event });
    }
    if (method === 'PATCH') {
      const b = JSON.parse(body || 'null');
      if (!b || typeof b !== 'object') return err('invalid body', 400);
      const updated = dbUpdateEvent(eventId, {
        title: b.title !== undefined ? String(b.title).trim() : undefined,
        summary: b.summary !== undefined ? (String(b.summary).trim() || null) : undefined,
        time_span_start: b.time_span_start ?? b.time_start ?? undefined,
        time_span_end: b.time_span_end ?? b.time_end,
        themes: b.themes !== undefined ? (Array.isArray(b.themes) ? b.themes.map(String) : null) : undefined,
        source_block_ids: b.source_block_ids !== undefined ? (Array.isArray(b.source_block_ids) ? b.source_block_ids.map(Number).filter((n) => Number.isFinite(n)) : null) : undefined,
        importance: b.importance !== undefined ? Math.max(0, Math.min(1, Number(b.importance))) : undefined,
        event_type: b.event_type !== undefined ? (['moment', 'period', 'milestone', 'ritual'].includes(String(b.event_type)) ? String(b.event_type) : undefined) : undefined,
      });
      if (!updated) return err('event not found or no changes', 404);
      const event = dbGetEvent(eventId);
      dbMarkEventDirty(eventId);
      return json({ ok: true, event });
    }
    if (method === 'DELETE') {
      const ev = dbGetEvent(eventId);
      if (!ev) return err('event not found', 404);
      dbDeleteEvent(eventId);
      DB.prepare('DELETE FROM event_edges WHERE event_a = ? OR event_b = ?').run(eventId, eventId);
      deleteEventVectors(eventId);
      return json({ ok: true, deleted: true, id: eventId });
    }
    return err('method not allowed', 405);
  }
  const graphMatch = subpath.match(/^\/events\/(\d+)\/graph\/?$/);
  if (graphMatch) {
    if (method !== 'GET') return err('method not allowed', 405);
    const g = getEventGraph(Number(graphMatch[1]));
    if (!g) return err('event not found', 404);
    return json({ ok: true, ...g });
  }
  const pathMatch = subpath.match(/^\/events\/(\d+)\/path\/(\d+)\/?$/);
  if (pathMatch) {
    if (method !== 'GET') return err('method not allowed', 405);
    try {
      return json(archiveEventPath({
        from: Number(pathMatch[1]), to: Number(pathMatch[2]),
        k: url.searchParams.get('k'), max_hops: url.searchParams.get('max_hops'),
      }));
    } catch (e) {
      return err(`event_path failed: ${e.message}`, 400);
    }
  }
  // ── 事件网络 rebuild ──
  if (subpath === '/events/probe' || subpath === '/events/probe/') {
    if (method !== 'POST') return err('method not allowed', 405);
    const b = JSON.parse(body || '{}');
    const probeId = Number(b.event_id);
    if (!Number.isFinite(probeId) || probeId <= 0) return err('event_id required', 400);
    const ev = dbGetEvent(probeId);
    if (!ev) return err('event not found', 404);
    const specs = buildEventVectorSpecs(ev);
    const best = [];
    const seen = new Set();
    for (const spec of specs) {
      try {
        const vec = readVecRow('vec_events', spec.id);
        if (!vec) continue;
        const rows = DB.prepare(`SELECT rowid, distance FROM vec_events WHERE embedding MATCH ? AND k = ?`)
          .all(vecBind(vec), 20 + 10);
        for (const row of rows) {
          const rid = Number(row.rowid);
          const meta = DB.prepare('SELECT event_id FROM vec_events_meta WHERE rowid = ?').get(row.rowid);
          const mid = meta ? meta.event_id : Math.floor(rid / EVENT_ROWID_SCALE);
          if (!Number.isInteger(mid) || mid <= 0 || mid === probeId) continue;
          if (seen.has(mid)) continue;
          seen.add(mid);
          best.push({ event_id: mid, score: Math.round((1 - row.distance) * 10000) / 10000, kind: matchKindOf(rid) });
        }
      } catch { /* 单条失败不阻塞 */ }
    }
    best.sort((a, b) => b.score - a.score);
    return json({ ok: true, event_id: probeId, title: ev.title, vector_count: specs.length, top_matches: best.slice(0, 12), threshold: THEMATIC_THRESHOLD });
  }
  if (subpath === '/associate' || subpath === '/associate/') {
    if (method !== 'POST') return err('method not allowed', 405);
    const b = JSON.parse(body || '{}');
    try {
      return json(await archiveAssociate(b));
    } catch (e) {
      return err(`associate failed: ${e.message}`, 400);
    }
  }
  if (subpath === '/events/network-stats' || subpath === '/events/network-stats/') {
    if (method !== 'GET') return err('method not allowed', 405);
    return json({ ok: true, ...dbEventNetworkStats() });
  }
  if (subpath === '/events/rebuild-network' || subpath === '/events/rebuild-network/') {
    if (method !== 'POST') return err('method not allowed', 405);
    const r = await rebuildEventNetwork();
    return json({ ok: true, ...r, dirty_remaining: r.embed_failures.length });
  }
  if (subpath === '/events/rebuild-network/stream' || subpath === '/events/rebuild-network/stream/') {
    if (method !== 'POST') return err('method not allowed', 405);
    return {
      __stream: async (res) => {
        res.writeHead(200, {
          'Content-Type': 'application/x-ndjson',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'no-store',
        });
        const send = async (e) => { try { res.write(JSON.stringify(e) + '\n'); } catch {} };
        try {
          const r = await rebuildEventNetwork(send);
          await send({ phase: 'done', ok: true, ...r, dirty_remaining: r.embed_failures.length });
        } catch (e) {
          await send({ phase: 'error', ok: false, error: e.message });
        }
        try { res.end(); } catch {}
      },
    };
  }
  if (subpath === '/events/restore-network' || subpath === '/events/restore-network/') {
    if (method !== 'POST') return err('method not allowed', 405);
    const r = restoreEventNetwork();
    if (!r) return err('没有可恢复的备份(rebuild 成功时备份已删除)', 404);
    return json({ ok: true, ...r });
  }
  // ── 社区(故事群) ──
  if (subpath === '/communities' || subpath === '/communities/') {
    if (method !== 'GET') return err('method not allowed', 405);
    const results = DB.prepare('SELECT * FROM event_communities ORDER BY internal_edges DESC, id ASC').all();
    const ids = new Set();
    for (const r of results) for (const m of JSON.parse(r.member_ids)) ids.add(m);
    const titleMap = dbEventTitleMap([...ids]);
    return json({
      ok: true, total: results.length,
      communities: results.map((r) => {
        const members = JSON.parse(r.member_ids);
        return {
          id: r.id, name: r.name, renamed: r.renamed === 1, note: r.note,
          member_ids: members,
          member_titles: members.map((m) => titleMap.get(m) ?? `#${m}`),
          core_event_id: r.core_event_id, internal_edges: r.internal_edges,
          time_span_start: r.time_span_start, time_span_end: r.time_span_end,
        };
      }),
    });
  }
  const commMatch = subpath.match(/^\/communities\/(\d+)\/?$/);
  if (commMatch) {
    const cid = parseInt(commMatch[1], 10);
    if (method === 'PATCH') {
      const b = JSON.parse(body || 'null');
      const name = String(b.name ?? '').trim();
      const note = b.note !== undefined ? String(b.note) : null;
      const r = DB.prepare('UPDATE event_communities SET name = ?, note = ?, renamed = ?, updated_at = ? WHERE id = ?')
        .run(name, note, name ? 1 : 0, ts(), cid);
      if (r.changes === 0) return err('community not found', 404);
      return json({ ok: true, id: cid, name, note });
    }
    if (method === 'DELETE') {
      const removed = DB.prepare('DELETE FROM event_communities WHERE id = ?').run(cid).changes;
      if (removed === 0) return err('community not found', 404);
      return json({ ok: true, removed });
    }
    return err('method not allowed', 405);
  }
  // ── night(助手的夜间整理: 唤醒开关 + 晨报; APP+ADMIN 平权; night.js 上报走 ADMIN) ──
  // GET  /night              → 开关状态 + 最近晨报摘要
  // POST /night {enabled}    → 切换夜间唤醒(04:00 agent-night)
  // GET  /night-reports      → 晨报列表(摘要, 日期倒序)
  // GET  /night-reports/:date→ 单晚晨报全文(判定明细/理由/故事群改名)
  // POST /night-report       → night.js 收尾上报(ADMIN), 按日期整份覆盖
  if (subpath === '/night' || subpath === '/night/') {
    if (method === 'GET') {
      const sw = dbNightSwitch();
      const last = DB.prepare('SELECT * FROM night_reports ORDER BY date DESC LIMIT 1').get();
      return json({ ok: true, enabled: sw.enabled, updated_at: sw.updated_at, last_report: last ? dbNightReportSummary(last) : null });
    }
    if (method === 'POST') {
      const b = JSON.parse(body || 'null');
      if (!b || typeof b.enabled !== 'boolean') return err('需要 {enabled: true|false}');
      const sw = dbNightSetEnabled(b.enabled);
      return json({ ok: true, ...sw });
    }
    return err('method not allowed', 405);
  }
  if (subpath === '/night-reports' && method === 'GET') {
    const limit = Math.min(Number(url.searchParams.get('limit') ?? 60) || 60, 1000);
    const rows = DB.prepare('SELECT * FROM night_reports ORDER BY date DESC LIMIT ?').all(limit);
    return json({ ok: true, items: rows.map(dbNightReportSummary), count: rows.length });
  }
  const nightMatch = subpath.match(/^\/night-reports\/(\d{4}-\d{2}-\d{2})\/?$/);
  if (nightMatch) {
    if (method !== 'GET') return err('method not allowed', 405);
    const row = DB.prepare('SELECT * FROM night_reports WHERE date = ?').get(nightMatch[1]);
    if (!row) return err('report not found', 404);
    let detail = {};
    try { detail = JSON.parse(row.detail_json || '{}'); } catch {}
    return json({ ok: true, report: { ...dbNightReportSummary(row), dream_words: detail.dream_words ?? null, verdicts: detail.verdicts ?? [], renames: detail.renames ?? [], notes: detail.notes ?? [], timeline: detail.timeline ?? [], tokens: detail.tokens ?? null } });
  }
  if (subpath === '/night-report' && method === 'POST') {
    if (!isAdmin) return err('unauthorized', 401);
    const b = JSON.parse(body || 'null');
    if (!b || typeof b.date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(b.date)) return err('需要 {date: "YYYY-MM-DD", verdicts, renames, notes, queue_left, elapsed_min}');
    const row = dbNightUpsertReport(b);
    return json({ ok: true, report: dbNightReportSummary(row) });
  }

  // ── profile(档案卡: 助手的自我叙事/关系认知 → system prompt; APP+ADMIN 平权, 用户经 REST 直接改人格画像) ──
  // GET /profile            → 分卡列表(按 id 顺序)
  // GET /profile?mode=assembled → 按 id 拼接的系统提示词全文(注入端用)
  // PUT/PATCH /profile/:title   → 整卡覆盖建档/改内容; {new_title} 改名, {new_id} 移动
  // PUT/PATCH /profile/:id      → 同上, 纯数字段按 id 定位
  if (subpath === '/profile' || subpath === '/profile/') {
    if (method !== 'GET') return err('method not allowed', 405);
    if (url.searchParams.get('mode') === 'assembled') {
      const items = dbListProfile();
      return json({ ok: true, mode: 'assembled', count: items.length, prompt: dbAssembleProfile() });
    }
    const items = dbListProfile();
    return json({ ok: true, count: items.length, items });
  }
  const profMatch = subpath.match(/^\/profile\/([^\/]+)\/?$/);
  if (profMatch) {
    if (method !== 'PUT' && method !== 'PATCH') return err('method not allowed', 405);
    const rawSeg = decodeURIComponent(profMatch[1]).trim();
    // 纯数字段 = 按 id 定位; 否则按 title
    const target = /^\d+$/.test(rawSeg) ? { id: parseInt(rawSeg, 10) } : { title: rawSeg };
    const b = JSON.parse(body || 'null');
    if (!b || typeof b !== 'object') return err('invalid body', 400);
    const hasContent = b.content != null && String(b.content).trim() !== '';
    const newTitle = b.new_title != null ? String(b.new_title).trim() : null;
    const newId = b.new_id != null ? Number(b.new_id) : null;
    if (!hasContent && !newTitle && newId == null) return err('没有要改的东西——content / new_title / new_id 至少传一个', 400);
    let card = dbGetProfile(target);
    // 建档只发生在: 按 title 定位 + 不存在 + 只改内容(老 PUT 行为不变)
    if (!card) {
      if (target.id != null || newTitle || newId != null) return err('profile card not found', 404);
      const r = dbUpsertProfile(target.title, String(b.content));
      return json({ ok: true, id: r.card.id, title: r.card.title, created: true, content_len: r.card.content.length, updated_at: r.card.updated_at });
    }
    if (hasContent) {
      DB.prepare('UPDATE profile SET content = ?, updated_at = ? WHERE title = ?').run(String(b.content), ts(), card.title);
    }
    if (newTitle) {
      const r = dbRenameProfile({ title: card.title }, newTitle);
      if (r.error) return err(r.error, r.status);
      card = r.card;
    }
    if (newId != null) {
      const r = dbMoveProfile({ title: card.title }, newId);
      if (r.error) return err(r.error, r.status);
      card = r.card;
    }
    card = dbGetProfile({ title: card.title });
    return json({ ok: true, id: card.id, title: card.title, created: false, content_len: card.content.length, updated_at: card.updated_at });
  }
  // ── 手动边 ──
  if (subpath === '/events/link' && method === 'POST') {
    const b = JSON.parse(body || 'null');
    if (!b || typeof b !== 'object') return err('invalid json body', 400);
    const r = createManualLink(b);
    if (r.error) return err(r.error, r.status ?? 400);
    return json({ ok: true, edge: r.edge });
  }
  if (subpath === '/events/link' && method === 'DELETE') {
    const a = Number(url.searchParams.get('event_a') ?? 0);
    const b = Number(url.searchParams.get('event_b') ?? 0);
    const type = String(url.searchParams.get('edge_type') ?? '');
    if (!Number.isFinite(a) || !Number.isFinite(b) || a <= 0 || b <= 0 || a === b) return err('需要两个不同的正整数 event id');
    if (type !== 'causal' && type !== 'overlap' && type !== 'thematic') return err('edge_type 必须是 causal / overlap / thematic');
    const removed = deleteManualLink(a, b, type);
    return json({ ok: true, removed });
  }
  return null;
}

const svc = startService({
  name: 'agent-archive',
  port: PORT,
  basePath: '/archive',
  appToken: CFG.appToken,
  adminToken: CFG.adminToken,
  maxBody: 4 * 1024 * 1024,
  tools: MCP_TOOLS,
  callTool,
  rest: (ctx) => handleRest({ ...ctx, isAdmin: svc.isAdmin, isAny: svc.isAny }),
});

module.exports = svc;
