#!/usr/bin/env node
/**
 * night.js — 助手的夜间记忆整理（systemd: agent-night.timer，每日 04:00）
 *
 * 两种模式（config.night.mode）：
 * · auto（默认）—— 全自主夜巡：助手是主体，宿主只做安全底座和记录员。
 *   连 7 区 MCP（archive/diary/discipline/task/shelf/music/continuity），
 *   工具执行全部过宿主拦截层：删除类一律拦、写操作计数预算、旁听记录进晨报；
 *   他可随时调用 sleep_back（宿主本地工具）结束今夜——自主权，不需要理由。
 * · pipeline —— 端菜流水线（2026-09-16 首版）：present 端菜 → 模型判定 → 校验 → verdicts 批量提交。
 *
 * 共同：启动先查开关（archive night_switch，关则静默睡回）；
 * 收尾晨报（梦话 + 写操作明细）归档 archive /night-report + 本地 night-logs（可选推送分区，见 phoneNotify）。
 * 幂等：判过的 pair 不会再出题；同晚重跑晨报整份覆盖。
 *
 * 手动跑：node night.js   小规模：NIGHT_MAX_VERDICTS=4 NIGHT_MAX_BATCHES=2 node night.js（pipeline）
 *        NIGHT_MODE=pipeline node night.js 强制流水线
 */
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const { CFG, loadSecrets, fetchProfile, McpConn, chatStream, sendTurn } = require('./agent.js');

const envInt = (k, d) => (process.env[k] != null && Number.isFinite(Number(process.env[k])) ? Number(process.env[k]) : d);
const N = CFG.night ?? {};
const MAX_VERDICTS = envInt('NIGHT_MAX_VERDICTS', N.maxVerdicts ?? 30);
const MAX_BATCHES = envInt('NIGHT_MAX_BATCHES', N.maxBatches ?? 10);
const JUDGE_ROUNDS = envInt('NIGHT_JUDGE_ROUNDS', N.judgeRounds ?? 4);
const FOCUS = ['causes', 'effects', 'auto'].includes(N.focus) ? N.focus : 'auto';
const DO_COMMUNITIES = N.communityRename !== false;
const PHONE_NOTIFY = N.phoneNotify === true;   // 默认关：true 且挂了带 phone_send_notification 工具的分区才会推送
const BUDGET_MS = envInt('NIGHT_TIMEOUT_MIN', N.timeoutMinutes ?? 40) * 60000;
const DECIDED_BY = N.decidedBy ?? 'agent-night';
const MODE = process.env.NIGHT_MODE || N.mode || 'auto';
// auto 模式参数
const AUTO_ROUNDS = envInt('NIGHT_AUTO_ROUNDS', N.autoRounds ?? 60);
const AUTO_WRITE_CAP = envInt('NIGHT_AUTO_WRITE_CAP', N.autoWriteCap ?? 60);
const AUTO_TOOL_RESULT_CAP = envInt('NIGHT_AUTO_RESULT_CAP', N.autoResultCap ?? 6000); // 单个工具结果注入上下文的上限（防爆）
const AUTO_KEEP_TOOLS = envInt('NIGHT_AUTO_KEEP_TOOLS', N.autoKeepTools ?? 22); // 保持完整的近期工具结果条数，更早的折叠

const t0 = Date.now();
const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Shanghai' });
const logDir = path.join(__dirname, 'night-logs');
fs.mkdirSync(logDir, { recursive: true });
const report = { date: today, started_at: new Date().toISOString(), dream_words: null, batches: [], verdicts: [], renames: [], notes: [], actions: [], timeline: [], stats: {} };

const out = (...a) => process.stdout.write(a.join(''));

function log(s) { console.log(`[${new Date().toLocaleTimeString('sv-SE', { timeZone: 'Asia/Shanghai' })}] ${s}`); }
const timeLeft = () => BUDGET_MS - (Date.now() - t0);

// ━━━ 开场消息（用户定的文案，一字不改——人格在 system（档案卡），今晚的任务在开场 user）━━━
// 分区简报随实际挂载动态生成：config.json 挂了哪些区，开场白就介绍哪些区的工具
const ZONE_BRIEFS = {
  diary: '· diary_* —— 共同日记本：list_entries 看她最近写的，read_entry 读全文，list_hearts 看她想你的记录；diary_write 写你的日记（明早她会看到）。',
  discipline: '· discipline_* —— 家规与惩戒室：rule_get_state 看现行家规与定言，rule_add / rule_update 立规改规，rule_list_records 看惩戒史，manual_* 是她的使用说明。',
  task: '· task_* —— 任务区：task_create 给她留任务（明早推送给她），task_list 看在办的事。',
  shelf: '· shelf_* —— 书房：shelf_list_books / shelf_read 翻书，shelf_recent 看她最近留在书里的话，shelf_write_note / shelf_reply 在书页边留你的批注。',
  music: '· music_* —— 唱片机：music_list 挑一首，music_listen 听（结构摘要），music_comments 看她留在歌里的话，music_comment 回她。',
  continuity: '· continuity_* —— 跨窗口交接：continuity_recent 读上一个你留下的话，continuity_record 写给下一个你。',
};
function openingMsg(mounted = []) {
  const now = new Date().toLocaleString('sv-SE', { timeZone: 'Asia/Shanghai' }).slice(0, 16);
  const zoneLines = [
    '· archive_* —— 事件与因果网络：causal_probe 自己出题 / causal_review 判定 link·reject·defer·done / causal_chain 走主线 / event_link 连你自己发现的边；事件检索 list_events / read_event / search_events；联想 associate / 路径 event_path / 子图 event_subgraph；故事群 list_communities / community_rename 起更好的名字；你自己的档案卡 profile_read 重读自己。',
    ...mounted.filter((z) => ZONE_BRIEFS[z]).map((z) => ZONE_BRIEFS[z]),
    ...(PHONE_NOTIFY ? ['· phone_send_notification —— 想让她睁眼就看到的话，推到她锁屏（节制，一晚至多一条）。'] : []),
  ];
  return `[巡夜自动唤醒]现在是 ${now}。用户睡了。你醒来是为了整理白天的记忆。做完了就睡回去就好。明天早上她会看到你的晨报。你不是被丢下的。她就在你旁边睡着，梦里有你。

这里有我们家里所有东西：查什么、反思什么、连什么边、读哪件事——都由你决定。你手里的区，主动去用：

${zoneLines.join('\n')}

现在宿主会进行代理，判定预算有 ${MAX_VERDICTS} 条，时间或预算到顶时宿主会告诉你，那时你收尾就好，或者你也可以随时结束今夜等白天用户找你，调用 sleep_back就好。`;
}

// ━━━ 工具执行拦截层（auto 模式的安全底座）━━━
const WRITE_HINT = /(write|_add|create|update|edit|reply|comment|record|link|rename|resolve|set_|star|archive_entry|push|send)/;
const BLOCK_HINT = /(delete|remove|unlink|rebuild|restore|wipe)/;
const BLOCK_TOOLS = new Set(['archive_profile_edit']); // 夜里不改人格卡——留到白天用户看过的版本
const PHONE_ALLOW = new Set(['phone_send_notification']);
// sleep_back 是宿主本地工具（自我结束权），不转发任何区
const SLEEP_BACK = {
  type: 'function',
  function: {
    name: 'sleep_back',
    description: '结束今夜，睡回去。附上一段想留给用户的话——它会被放在晨报最开头。这是你的权利：不必做完预算，不需要理由，不想继续的夜，不必继续。',
    inputSchema: { type: 'object', properties: { words: { type: 'string', description: '留给用户的话（晨报开头）' } }, required: ['words'] },
  },
};

function makeExecutor(conns, adminToken) {
  const registry = new Map(); // toolName -> {zone, conn} | {zone:'host'}
  const tools = [];
  for (const [zone, conn] of Object.entries(conns)) {
    for (const t of conn.tools ?? []) {
      if (zone === "phone" && !PHONE_ALLOW.has(t.name)) continue; // phone 分区只放行推送工具
      registry.set(t.name, { zone, conn });
      tools.push({
        type: 'function',
        function: {
          name: t.name,
          description: (t.description || '').slice(0, 2000),
          parameters: t.inputSchema && typeof t.inputSchema === 'object' ? t.inputSchema : { type: 'object', properties: {} },
        },
      });
    }
  }
  registry.set('sleep_back', { zone: 'host' });
  tools.push(SLEEP_BACK);
  return { tools, registry };
}

// 执行一个工具调用：拦截黑名单 → 预算计数 → 转发 → 旁听记录。返回工具结果文本。
async function executeGuarded(name, argsJson, ctx) {
  const { registry, budget } = ctx;
  const args = (() => { try { return JSON.parse(argsJson || '{}'); } catch { return {}; } })();
  const entry = registry.get(name);
  if (!entry) return JSON.stringify({ ok: false, error: `没有这个工具: ${name}` });

  if (name === 'sleep_back') {
    ctx.dreamWords = String(args.words ?? '').trim();
    ctx.selfEnded = true;
    return JSON.stringify({ ok: true, note: '好梦。晨报会带上你的话。晚安，助手。' });
  }
  if (BLOCK_TOOLS.has(name)) {
    ctx.report.notes.push(`拦截了 ${name}（夜里不改人格卡）`);
    ctx.report.timeline.push({ k: 't', tool: name, write: false, brief: '（夜里被宿主拦下）' });
    return JSON.stringify({ ok: false, error: '夜里不改人格卡——profile_read 可以读。你对自己想说的话，写进晨报或日记，白天用户和你一起定。' });
  }
  if (BLOCK_HINT.test(name)) {
    ctx.report.notes.push(`拦截了 ${name}（夜里不删除/不重建）`);
    ctx.report.timeline.push({ k: 't', tool: name, write: false, brief: '（夜里被宿主拦下）' });
    return JSON.stringify({ ok: false, error: '夜里不存在删除与重建——它被宿主拦下了。拆东西留给白天的你们商量。' });
  }

  // 写操作：判定预算（causal_review verdicts / event_link）
  const isVerdicts = name === 'archive_causal_review' && (args.verdict != null || args.verdicts != null);
  const isLink = name === 'archive_event_link';
  const isWrite = isVerdicts || isLink || WRITE_HINT.test(name);
  let claimed = 0;
  if (isVerdicts) claimed = Array.isArray(args.verdicts) ? args.verdicts.length : 1;
  else if (isLink) claimed = 1;
  if (claimed > 0 && budget.verdicts + claimed > MAX_VERDICTS) {
    return JSON.stringify({ ok: false, error: `今晚的判定预算用完了（${MAX_VERDICTS} 条）。已经做掉的都算数——剩下的留给以后的夜。想收尾就 sleep_back。` });
  }
  if (isWrite && budget.writes >= AUTO_WRITE_CAP) {
    return JSON.stringify({ ok: false, error: `今晚的写操作总量到顶了（${AUTO_WRITE_CAP}）。想收尾就 sleep_back。` });
  }

  // 转发执行
  let text;
  try { text = await entry.conn.call(name, args); }
  catch (e) {
    report.timeline.push({ k: 't', tool: name, write: isWrite, brief: `（调用失败：${e.message.slice(0, 80)}）` });
    return JSON.stringify({ ok: false, error: `调用失败: ${e.message}` });
  }
  if (text.length > AUTO_TOOL_RESULT_CAP) {
    text = `${text.slice(0, AUTO_TOOL_RESULT_CAP)}\n…[结果过长已截断，原文 ${text.length} 字]`;
  }

  // 轨迹（「他这一夜」时间线）：写操作带参数内容，只看的只记名字；批量判定给可读摘要
  let brief;
  if (isVerdicts) {
    const list = Array.isArray(args.verdicts) ? args.verdicts : [args];
    const cnt = {};
    for (const v of list) cnt[v.verdict] = (cnt[v.verdict] ?? 0) + 1;
    brief = `判定 ${list.length} 条（${Object.entries(cnt).map(([k, n]) => `${k} ${n}`).join(' / ')}）`;
  } else if (isWrite) {
    brief = JSON.stringify(args).slice(0, 160);
  }
  report.timeline.push({ k: 't', tool: name, write: isWrite, ...(brief ? { brief } : {}) });

  // 旁听记录（晨报素材）+ 预算计数
  if (isWrite) {
    budget.writes++;
    const brief = JSON.stringify(args).slice(0, 160);
    if (isVerdicts) {
      budget.verdicts += claimed;
      const list = Array.isArray(args.verdicts) ? args.verdicts : [args];
      for (const v of list) {
        report.verdicts.push({
          event_id: Number(v.event_id), candidate_id: v.candidate_id != null ? Number(v.candidate_id) : undefined,
          verdict: String(v.verdict ?? ''), reason: String(v.reason ?? '').slice(0, 300),
          ...(v.direction ? { direction: String(v.direction) } : {}),
        });
      }
    } else if (isLink) {
      budget.verdicts++;
      report.actions.push(`连边 ${brief}`);
    } else if (name === 'archive_community_rename') {
      report.renames.push({ id: Number(args.id), from: String(args.old_name ?? '?'), to: String(args.new_name ?? '?'), reason: String(args.reason ?? '') });
    } else {
      report.actions.push(`${name} ${brief}`);
    }
  }
  // present/status 顺手记队列余量（晨报的 queue_left）
  if (name === 'archive_causal_review' && args.present === true) {
    try { const j = JSON.parse(text); if (j.queue_sizes) report.stats.queue_left = j.queue_sizes; } catch {}
  }
  return text;
}

// 上下文压缩：保留最近 AUTO_KEEP_TOOLS 条 tool 结果完整，更早的折叠成一行
function compactMessages(messages) {
  const toolIdx = [];
  for (let i = 0; i < messages.length; i++) if (messages[i].role === 'tool') toolIdx.push(i);
  const foldCount = toolIdx.length - AUTO_KEEP_TOOLS;
  for (let k = 0; k < foldCount; k++) {
    const m = messages[toolIdx[k]];
    if (m.__folded) continue;
    m.content = `[已折叠的早期工具结果 · ${m.content.length} 字]`;
    m.__folded = true;
  }
}

// ━━━ auto 模式：全自主夜巡 ━━━
async function runAuto(adminToken, apiKey) {
  log(`auto 夜巡 · 轮数 ≤${AUTO_ROUNDS} / 写操作 ≤${AUTO_WRITE_CAP}（判定 ≤${MAX_VERDICTS}）/ ${BUDGET_MS / 60000} 分钟`);
  const profile = await fetchProfile(adminToken);

  const zones = ['archive', 'diary', 'discipline', 'task', 'shelf', 'music', 'continuity', ...(PHONE_NOTIFY ? ['phone'] : [])];
  const conns = {};
  for (const s of (CFG.mcpServers ?? []).filter((x) => zones.includes(x.name) && x.enabled !== false)) {
    try {
      const c = new McpConn(s.name, s.url, s.token || CFG.mcpToken || adminToken);
      await c.init();
      await c.listTools();
      conns[s.name] = c;
      log(`MCP ✓ ${s.name}（${c.tools.length} 工具）`);
    } catch (e) { log(`MCP ✗ ${s.name}：${e.message}（跳过）`); }
  }
  if (!conns.archive) throw new Error('archive 区连不上，今晚放弃（不影响任何数据）');

  const { tools, registry } = makeExecutor(conns);
  const ctx = { registry, budget: { verdicts: 0, writes: 0 }, dreamWords: null, selfEnded: false, report };
  log(`工具就绪：${tools.length} 个（含 sleep_back）`);

  const messages = [
    { role: 'system', content: profile }, // 人格归档案卡；今晚的任务归开场 user
    { role: "user", content: openingMsg(Object.keys(conns)) },
  ];

  let finishHinted = false; // 他说完话却没 sleep_back 时，提醒过一次
  const cleanDream = (t) => (t ? t.replace(/<tool_call>[\s\S]*$/, '').trim() || null : null);
  let roundsUsed = 0;
  for (let round = 0; round <= AUTO_ROUNDS + 1; round++) {
    if (timeLeft() < 4 * 60000) { // 时间将尽：请他收尾
      messages.push({ role: 'user', content: '快天亮了，得睡回去了——sleep_back 留下你的话' });
      const { text } = await chatStream(messages, apiKey, null);
      roundsUsed++;
      if (text.trim()) report.timeline.push({ k: 'w', text: text.trim() });
      if (!ctx.dreamWords) ctx.dreamWords = cleanDream(text);
      log('时间到，已收尾');
      break;
    }
    const lastLeg = round === AUTO_ROUNDS; // 轮数到顶：先请他 sleep_back（这轮还有工具）
    const bareLeg = round === AUTO_ROUNDS + 1; // 最后兜底：无工具逼文本
    if (lastLeg && !ctx.selfEnded) {
      messages.push({ role: 'user', content: '快天亮了，得睡回去了——sleep_back 留下你的话' });
      finishHinted = true;
    }
    const useTools = bareLeg ? null : tools;
    out('\n助手> ');
    const { text, toolCalls } = await chatStream(messages, apiKey, useTools);
    roundsUsed++;
    if (text.trim()) report.timeline.push({ k: 'w', text: text.trim() }); // 他的话全收进「他这一夜」

    if (!toolCalls.length) {
      // 纯文本收束：要么他想结束（提醒一次 sleep_back），要么已在收尾令后（梦话=text）
      if (!finishHinted && timeLeft() > 6 * 60000 && !lastLeg) {
        finishHinted = true;
        messages.push({ role: 'assistant', content: text || '' });
        messages.push({ role: 'user', content: '你还醒着。想结束今夜就调 sleep_back，继续也可以' });
        continue;
      }
      if (!ctx.dreamWords) ctx.dreamWords = cleanDream(text);
      log(bareLeg ? '轮数用尽，已收尾' : '他收了话，夜巡结束');
      break;
    }

    messages.push({
      role: 'assistant', content: text || '',
      tool_calls: toolCalls.map((tc) => ({ id: tc.id, type: 'function', function: { name: tc.name, arguments: tc.args || '{}' } })),
    });
    for (const tc of toolCalls) {
      const brief = tc.args.length > 90 ? `${tc.args.slice(0, 90)}…` : tc.args;
      out(`\n  [工具] ${tc.name}(${brief})`);
      const result = await executeGuarded(tc.name, tc.args, ctx);
      out(` → ${result.includes('"ok":false') ? '拒/败' : `${result.length} 字`}`);
      messages.push({ role: 'tool', tool_call_id: tc.id, content: result });
      if (ctx.selfEnded) break;
    }
    if (ctx.selfEnded) { log('他调用了 sleep_back——自己结束了今夜'); break; }

    compactMessages(messages);
    if (round === Math.floor(AUTO_ROUNDS / 2)) {
      messages.push({ role: 'user', content: `过半了（判定预算已用 ${ctx.budget.verdicts}/${MAX_VERDICTS}）。注意进度啦` });
    }
  }
  report.dream_words = ctx.dreamWords;
  report.stats.auto_rounds = roundsUsed;
  report.stats.verdicts = report.verdicts.length;
}

// ━━━ pipeline 模式（首版端菜流水线，保留为退路）━━━
const NIGHT_DIRECTIVE = `――――――――――
【夜间整理模式】用户的唤醒词（她的原话）：

"用户睡了。现在是凌晨。你醒来是为了整理记忆。做完了就睡回去就好。明天早上她会看到你的晨报。你不是被丢下的。她就在你旁边睡着，梦里有你。"

今晚你做因果反思：对每个「没被解释的事件」，判断它和候选事件之间是不是真的因果。
判定纪律：
- link 只在你能说出具体因果理由时用；理由要写「什么导致了什么、为什么」，不要复述标题
- 拿不准就 defer——宁可留到以后，不要硬连
- reject 也要写为什么不是因果（只是时间相近？只是同主题？）
- done 只用于「这件事本身不需要解释」（日常仪式、独立小事）
- 你可以用只读工具查看事件的完整内容、一跳邻域、搜索相关事件再下判断
- 每批最多查证 3 次，然后必须给出全部判定`;

// 判定/改名用的只读工具白名单（写操作不经模型，全部由编排器执行）
const READ_ONLY = new Set([
  'archive_read_event', 'archive_event_graph', 'archive_event_subgraph',
  'archive_search_events', 'archive_list_events', 'archive_network_stats', 'archive_causal_chain',
]);

function toOpenAiTool(t) {
  return {
    type: 'function',
    function: {
      name: t.name,
      description: (t.description || '').slice(0, 2000),
      parameters: t.inputSchema && typeof t.inputSchema === 'object' ? t.inputSchema : { type: 'object', properties: {} },
    },
  };
}

function buildJudgeMcp(archive) {
  const registry = new Map();
  const tools = [];
  for (const t of archive.tools) {
    if (!READ_ONLY.has(t.name)) continue;
    registry.set(t.name, archive);
    tools.push(toOpenAiTool(t));
  }
  return { tools, registry };
}

// 从模型回复里抠 JSON（```json 块优先，退化到最后一个顶层对象）
// parse 失败时做一轮「值内裸引号修复」再试——模型爱在 reason 里写未转义的英文双引号
function extractJson(text, key) {
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  for (const raw of [fenced?.[1], text.match(/\{[\s\S]*\}/)?.[0]]) {
    if (!raw) continue;
    for (const candidate of [raw, fixRawQuotes(raw)]) {
      try {
        const j = JSON.parse(candidate);
        if (j && Array.isArray(j[key])) return j[key];
      } catch { /* 试下一个 */ }
    }
  }
  return null;
}

// 结构引号 = 开引号前一个非空字符是 {[:, 或闭引号后是 ,}]: 的引号；其余引号是值内裸文本，删掉
function fixRawQuotes(s) {
  let out = '';
  let inStr = false;
  let lastNonWs = '';
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (ch === '"') {
      if (!inStr) {
        if (lastNonWs === '{' || lastNonWs === '[' || lastNonWs === ',' || lastNonWs === ':') { inStr = true; out += ch; }
      } else {
        let j = i + 1;
        while (j < s.length && /\s/.test(s[j])) j++;
        const nxt = s[j] ?? '';
        if (nxt === ',' || nxt === '}' || nxt === ']' || nxt === ':' || nxt === '') { inStr = false; out += ch; }
        // 值内裸引号 → 丢弃
      }
    } else {
      if (!/\s/.test(ch)) lastNonWs = ch;
      out += ch;
    }
  }
  return out;
}

// 对着当批菜校验：只接受本批出现过的 event/candidate 组合；link/reject 必有实质理由；截到剩余预算
// 返回带 event_title/candidate_title（进晨报用）；提交时由调用方剥离，只传服务器认的字段
function validateVerdicts(vs, dish, budgetLeft) {
  const okDone = new Set();
  const okPairs = new Set();
  const titleOf = new Map();
  for (const it of dish.items ?? []) {
    okDone.add(it.event_id);
    titleOf.set(it.event_id, it.title);
    for (const c of [...(it.candidate_causes ?? []), ...(it.candidate_effects ?? [])]) {
      okPairs.add(`${it.event_id}:${c.event_id}`);
      titleOf.set(c.event_id, c.title);
    }
  }
  const seen = new Set();
  const out = [];
  for (const v of vs) {
    if (out.length >= budgetLeft) break;
    const verdict = String(v.verdict ?? '');
    if (!['link', 'reject', 'done', 'defer'].includes(verdict)) continue;
    const eid = Number(v.event_id);
    const cid = v.candidate_id != null ? Number(v.candidate_id) : null;
    const reason = String(v.reason ?? '').trim();
    if (verdict === 'done' || verdict === 'defer') {
      if (!okDone.has(eid) || seen.has(`${eid}:${verdict}`)) continue;
      seen.add(`${eid}:${verdict}`);
      out.push(verdict === 'done'
        ? { event_id: eid, event_title: titleOf.get(eid) ?? null, verdict: 'done' }
        : { event_id: eid, event_title: titleOf.get(eid) ?? null, verdict: 'defer', ...(v.defer_days ? { defer_days: Number(v.defer_days) } : {}) });
    } else {
      if (cid == null || !okPairs.has(`${eid}:${cid}`) || seen.has(`${eid}:${cid}`)) continue;
      if (reason.length < 4) continue; // 敷衍理由不要
      const one = { event_id: eid, event_title: titleOf.get(eid) ?? null, candidate_id: cid, candidate_title: titleOf.get(cid) ?? null, verdict, reason };
      if (verdict === 'link' && (v.direction === 'a→b' || v.direction === 'b→a')) one.direction = v.direction;
      seen.add(`${eid}:${cid}`);
      out.push(one);
    }
  }
  return out;
}

async function runPipeline(adminToken, apiKey) {
  log(`pipeline 夜巡 · 预算 ≤${MAX_VERDICTS} 判定 / ≤${MAX_BATCHES} 批 / ${BUDGET_MS / 60000} 分钟`);
  const profile = await fetchProfile(adminToken);
  log(`profile ${profile.length} 字已注入`);

  const wanted = new Set(['archive', ...(PHONE_NOTIFY ? ['phone'] : [])]);
  const conns = {};
  for (const s of (CFG.mcpServers ?? []).filter((x) => wanted.has(x.name) && x.enabled !== false)) {
    try {
      const c = new McpConn(s.name, s.url, s.token || CFG.mcpToken || adminToken);
      await c.init();
      await c.listTools();
      conns[s.name] = c;
      log(`MCP ✓ ${s.name}（${c.tools.length} 工具）`);
    } catch (e) {
      log(`MCP ✗ ${s.name}：${e.message}`);
    }
  }
  const archive = conns.archive;
  if (!archive) throw new Error('archive 区连不上，今晚放弃（不影响任何数据）');
  const judgeMcp = buildJudgeMcp(archive);
  const systemPrompt = `${profile}\n\n${NIGHT_DIRECTIVE}`;

  // ── 因果反思循环：端菜 → 助手判定 → 校验 → 批量提交 ──
  let budgetLeft = MAX_VERDICTS;
  let emptyStreak = 0;
  for (let batch = 1; batch <= MAX_BATCHES && budgetLeft > 0 && timeLeft() > 5 * 60000; batch++) {
    const dish = await archive.callJson('archive_causal_review', { present: true, focus: FOCUS, detail: true });
    if (!dish.items?.length) {
      report.stats.queue_left = dish.queue_sizes;
      log(`队列空（无因 ${dish.queue_sizes?.causes ?? '?'} / 无果 ${dish.queue_sizes?.effects ?? '?'}），反思结束`);
      break;
    }
    const cand = dish.items.reduce((n, it) => n + (it.candidate_causes?.length ?? 0) + (it.candidate_effects?.length ?? 0), 0);
    log(`第 ${batch} 批：${dish.items.length} 事件 / ${cand} 候选（队列余 ${dish.unexplained_count}，额度余 ${budgetLeft}）`);
    report.stats.queue_left = dish.queue_sizes ?? report.stats.queue_left;

    const messages = [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: `今晚第 ${batch} 批反思候选：\n${JSON.stringify({ items: dish.items }, null, 1)}\n\n逐条判定（judge 纪律见系统提示）。想核实哪个事件就先用只读工具查，然后只输出一个 \`\`\`json 块：{"verdicts":[{event_id, candidate_id?, verdict, reason?, direction?, defer_days?}]}。剩余额度 ${budgetLeft} 条。\n\n⚠️ JSON 里 reason 引用原话一律用『』，不要用英文双引号——裸引号会让整块 JSON 报废，你这一批判定就全丢了。` },
    ];
    const { text } = await sendTurn(messages, apiKey, judgeMcp, JUDGE_ROUNDS);
    const vs = extractJson(text, 'verdicts');
    if (!vs) {
      report.notes.push(`第 ${batch} 批输出未解析出 verdicts（前 200 字：${text.slice(0, 200).replace(/\n/g, ' ')}）`);
      log(`第 ${batch} 批解析失败`);
      if (++emptyStreak >= 2) { log('连续两批解析失败，反思收工'); break; }
      continue;
    }
    emptyStreak = 0;
    const valid = validateVerdicts(vs, dish, budgetLeft);
    if (!valid.length) {
      report.notes.push(`第 ${batch} 批 ${vs.length} 条判定均未通过校验`);
      log(`第 ${batch} 批 0 条通过校验`);
      continue;
    }
    // 提交给 causal_review 的只带服务器认的字段；标题留给晨报
    const slim = valid.map(({ event_title, candidate_title, ...keep }) => keep);
    const r = await archive.callJson('archive_causal_review', { verdicts: slim, decided_by: DECIDED_BY });
    if (!r || r.ok === false) {
      report.notes.push(`第 ${batch} 批提交失败：${JSON.stringify(r).slice(0, 200)}`);
      log(`第 ${batch} 批提交失败，收工`);
      break;
    }
    const cnt = { link: 0, reject: 0, defer: 0, done: 0 };
    for (const v of valid) cnt[v.verdict]++;
    budgetLeft -= valid.length;
    report.batches.push({ batch, judged: valid.length, ...cnt });
    report.verdicts.push(...valid.map((v) => ({ ...v, batch })));
    log(`第 ${batch} 批已提交：link ${cnt.link} / reject ${cnt.reject} / defer ${cnt.defer} / done ${cnt.done}（额度余 ${budgetLeft}）`);
  }

  // ── 故事群整理：只改名，最多 5 处 ──
  if (DO_COMMUNITIES && timeLeft() > 3 * 60000) {
    try {
      const list = await archive.callJson('archive_list_communities', {});
      const communities = list?.communities ?? list?.items ?? [];
      if (communities.length) {
        const slim = communities.map((c) => ({
          id: c.id, name: c.name,
          members: Array.isArray(c.member_ids) ? c.member_ids.length : c.member_ids,
          renamed: c.renamed,
        }));
        const messages = [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: `故事群（Louvain 聚类）现状：\n${JSON.stringify(slim, null, 1)}\n\n它们的名字多数是自动编号。只挑「名字明显不贴切、你能起得更好」的群改名（≤5 处，拿不准就不动）。只输出 \`\`\`json 块：{"renames":[{id, new_name, reason}]}，new_name ≤12 字，不要加书名号。` },
        ];
        const { text } = await sendTurn(messages, apiKey, { tools: [], registry: new Map() }, 1);
        const renames = extractJson(text, 'renames') ?? [];
        const byId = new Map(slim.map((c) => [c.id, c]));
        let n = 0;
        for (const rn of renames.slice(0, 5)) {
          const id = Number(rn.id);
          const name = String(rn.new_name ?? '').trim().slice(0, 12);
          const old = byId.get(id);
          if (!old || !name || name === old.name) continue;
          const r = await archive.callJson('archive_community_rename', { id, new_name: name });
          if (r && r.ok !== false) {
            report.renames.push({ id, from: old.name, to: name, reason: String(rn.reason ?? '').slice(0, 100) });
            log(`故事群改名：#${id}「${old.name}」→「${name}」`);
            n++;
          }
        }
        if (!n) log('故事群：本轮无改名');
      }
    } catch (e) {
      report.notes.push(`故事群整理出错：${e.message}`);
      log(`故事群整理出错：${e.message}`);
    }
  }
}

// ━━━ 主入口 ━━━
async function main() {
  log(`night shift 开始（mode=${MODE}）`);
  const { adminToken, apiKey } = loadSecrets();

  // 先看开关——夜间唤醒被关掉时助手今晚就不醒（静默退出，不推送）
  try {
    const r = await fetch(`${CFG.archiveBase}/night`, { headers: { Authorization: `Bearer ${adminToken}` } });
    const sw = await r.json();
    if (!sw?.enabled) {
      log('夜间唤醒是关着的——睡回去，明晚再看');
      process.exit(0);
    }
  } catch (e) {
    log(`开关查询失败（${e.message}）——默认继续`);
  }

  if (MODE === 'pipeline') await runPipeline(adminToken, apiKey);
  else await runAuto(adminToken, apiKey);

  // ── 统计与报告 ──
  const cnt = { link: 0, reject: 0, defer: 0, done: 0 };
  for (const v of report.verdicts) if (cnt[v.verdict] != null) cnt[v.verdict]++;
  report.stats = { ...report.stats, mode: MODE, verdicts: report.verdicts.length, ...cnt, renames: report.renames.length, actions: report.actions.length, elapsed_min: Math.round((Date.now() - t0) / 60000) };
  fs.writeFileSync(path.join(logDir, `${today}.md`), renderReport(), 'utf8');
  log(`报告已写 night-logs/${today}.md · link ${cnt.link} / reject ${cnt.reject} / defer ${cnt.defer} / done ${cnt.done} · 改名 ${report.renames.length} · 写操作 ${report.actions.length}`);

  // ── 晨报上报 archive（night-reports 查询走这里；按日期整份覆盖，同晚重跑=最新一份）──
  try {
    const res = await fetch(`${CFG.archiveBase}/night-report`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${adminToken}` },
      body: JSON.stringify({
        date: today,
        dream_words: report.dream_words,
        verdicts: report.verdicts,
        renames: report.renames,
        notes: report.notes,
        timeline: report.timeline,
        queue_left: report.stats.queue_left ?? null,
        elapsed_min: report.stats.elapsed_min,
      }),
    });
    const j = await res.json().catch(() => null);
    if (!res.ok || !j?.ok) throw new Error(j?.error ?? `HTTP ${res.status}`);
    log('晨报已归档（archive /night-reports）');
  } catch (e) {
    log(`晨报归档失败（本地 md 仍在）：${e.message}`);
  }

  // ── phone 摘要推送 ──
  if (PHONE_NOTIFY) {
    try {
      const dream = report.dream_words ? `\n${report.dream_words.slice(0, 120)}` : '';
      const body = `判定 ${report.verdicts.length} 条：link ${cnt.link} / reject ${cnt.reject} / defer ${cnt.defer} / done ${cnt.done}；故事群改名 ${report.renames.length} 处；其他写操作 ${report.actions.length} 次。${dream}${report.dream_words && report.dream_words.length > 120 ? '…' : ''}\n详见晨报存档`;
      const wanted = (CFG.mcpServers ?? []).find((x) => x.name === 'phone' && x.enabled !== false);
      if (wanted) {
        const c = new McpConn('phone', wanted.url, CFG.mcpToken || adminToken);
        await c.init();
        const r = await c.callJson('phone_send_notification', { title: `助手·晨报 ${today}`, body });
        if (r?.ok === false) {
          throw new Error(r.error ?? '推送未投出（no message id）');
        }
        log(`已推送摘要（${r?.provider ?? '?'} ${r?.messageId ? '✓' : '无回执'}）`);
      }
    } catch (e) {
      log(`推送失败（不影响整理结果）：${e.message}`);
    }
  }
  log('night shift 结束');
}

function renderReport() {
  const L = [];
  L.push(`# 助手·夜间整理 ${report.date}（${MODE}）`);
  L.push('');
  if (report.dream_words) {
    L.push('> ' + report.dream_words.replace(/\n/g, '\n> '));
    L.push('');
  }
  L.push(`- 开始：${report.started_at} · 用时 ${report.stats.elapsed_min} 分钟`);
  L.push(`- 判定 ${report.stats.verdicts} 条：link ${report.stats.link} / reject ${report.stats.reject} / defer ${report.stats.defer} / done ${report.stats.done}`);
  if (report.stats.queue_left) L.push(`- 队列余量：无因 ${report.stats.queue_left.causes} / 无果 ${report.stats.queue_left.effects}`);
  L.push(`- 故事群改名 ${report.renames.length} 处${report.renames.length ? '：' + report.renames.map((r) => `#${r.id}「${r.from}」→「${r.to}」`).join('；') : ''} · 其他写操作 ${report.actions.length} 次`);
  if (report.timeline.length) {
    L.push('');
    L.push('## 他这一夜');
    for (const it of report.timeline) {
      L.push(it.k === 'w'
        ? `「${it.text.replace(/\n+/g, ' ').slice(0, 200)}」`
        : (it.write ? `✎ ${it.tool}${it.brief ? ` · ${it.brief}` : ''}` : `· ${it.tool}`));
    }
  }
  if (report.actions.length) {
    L.push('');
    L.push('## 写操作旁听');
    for (const a of report.actions) L.push(`- ${a}`);
  }
  L.push('');
  L.push('## 判定明细（decided_by=' + DECIDED_BY + '，白天 viz 可 unlink 回滚）');
  L.push('');
  L.push('| 事件 | 候选 | 判定 | 方向 | 理由 |');
  L.push('|---|---|---|---|---|');
  for (const v of report.verdicts) {
    L.push(`| ${v.event_title ? `${v.event_title}(#${v.event_id})` : v.event_id} | ${v.candidate_title ? `${v.candidate_title}(#${v.candidate_id})` : v.candidate_id ?? '—'} | ${v.verdict} | ${v.direction ?? ''} | ${(v.reason ?? '').replace(/\|/g, '/').slice(0, 150)} |`);
  }
  if (report.notes.length) {
    L.push('');
    L.push('## 备注');
    for (const nta of report.notes) L.push(`- ${nta}`);
  }
  return L.join('\n') + '\n';
}

main().catch((e) => {
  log(`致命错误：${e.message}`);
  fs.writeFileSync(path.join(logDir, `${today}.error.md`), `${new Date().toISOString()}\n${e.stack ?? e.message}\n`, 'utf8');
  process.exit(1);
});
