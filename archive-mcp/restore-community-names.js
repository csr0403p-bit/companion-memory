// restore-community-names.js —— 2026-08-22
// 从 D1 dump 恢复旧社区名（Jaccard ≥ 0.6 匹配到当前社区），一次性使用。
// 用法: sqlite3 /tmp/old.sqlite < archive-dump.sql && node restore-community-names.js
'use strict';
const { DatabaseSync } = require('node:sqlite');
const old = new DatabaseSync('/tmp/old.sqlite');
const path = require('node:path');
const fs = require('node:fs');
const CFG = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8'));
const live = new DatabaseSync(path.join(__dirname, CFG.dataDir || 'data', 'archive.sqlite'));

const olds = old.prepare('SELECT name, note, member_ids FROM event_communities').all()
  .filter((r) => r.name && !r.name.startsWith('未命名'))
  .map((r) => ({ name: r.name, note: r.note, members: new Set(JSON.parse(r.member_ids)) }));
const news = live.prepare('SELECT id, name, member_ids FROM event_communities').all()
  .map((r) => ({ id: r.id, name: r.name, members: new Set(JSON.parse(r.member_ids)) }));

let restored = 0;
const used = new Set();
for (const o of olds) {
  let best = null;
  let bestRatio = 0.6;
  for (const n of news) {
    if (used.has(n.id) || (n.name && !n.name.startsWith('未命名'))) continue;
    let inter = 0;
    for (const m of n.members) if (o.members.has(m)) inter++;
    const union = n.members.size + o.members.size - inter;
    const ratio = union > 0 ? inter / union : 0;
    if (ratio >= bestRatio) { bestRatio = ratio; best = n; }
  }
  if (best) {
    live.prepare('UPDATE event_communities SET name = ?, note = ?, renamed = 1, updated_at = ? WHERE id = ?')
      .run(o.name, o.note, new Date().toISOString(), best.id);
    used.add(best.id);
    restored++;
    console.log(`restored: ${o.name} -> community ${best.id} (J=${bestRatio.toFixed(2)})`);
  } else {
    console.log(`unmatched: ${o.name}`);
  }
}
console.log(`restored total: ${restored}`);
