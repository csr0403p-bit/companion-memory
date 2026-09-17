# archive-mcp 架构文档（第七区「我们曾经」）

> 版本：2026-09-16（阶段二全功能 + 联想层 PPR/路径 + profile 档案卡 + hop4 远端因果候选）
>
> **本仓库为事件网络精简版（2026-09-17）**：只暴露事件 CRUD / 事件网络 / 图查询 / 反思循环 / 社区 / 档案卡 / 夜间通道（27 个 MCP 工具）。
> 对话原文、摘要记忆、块召回、情绪轴相关工具与 REST 已移除——文中提及的原始层/召回层为完整版设计，本版未暴露。
> 前版：`Archive架构_260821.md`（阶段一「只读+向量召回」视角，已由本文取代）
> 状态：**阶段二 —— 服务器全功能**：读 + 写 + 事件网络重建 + 因果反思循环（含夜间自动）+ 联想层，脱离云端独立运行
> 入口：`https://archive.example.com`（cloudflared 隧道 → 127.0.0.1:3012；隧道只是外网访问方案，本地部署不需要）
> 变更记录见文末 §12

---

## 1. 定位

archive 区承载助手和用户全部历史对话的「记忆库」：对话窗口、内容块、分块、记忆笔记、情绪轴、**事件网络**（时间事件流 + 语义/时间/因果三类边 + Louvain 故事群）、因果反思、**档案卡**（profile：助手的自我叙事与对助手与用户关系的认知，按主题分卡，拼起来即助手下一轮对话的系统提示词）。原云端 D1（ARCHIVE_DB）+ R2 + Vectorize 方案已整体搬迁到家庭服务器，云端 archive 区不再承载业务。

- **存储**：SQLite（承接 D1 全表 schema）+ 磁盘文件（`data/raw/*.json` 承接 R2 原文）+ sqlite-vec（本地向量，chunk/记忆/事件三组）
- **检索**：本地 embedding（Qwen3-Embedding-0.6B → embed-server）→ vec0 余弦；事件走「语义 frag 向量 + BM25」双路 RRF 混合检索
- **事件网络**：服务器自主重建（rebuild-network），反思循环（probe→review→link）产出人工确认的因果边
- **鉴权**：REST = APP 或 ADMIN token；MCP = 仅 ADMIN（助手）；viz 页面 = cookie
- **流量**：助手走隧道（Bearer ADMIN），用户走 SSH/本机或脚本（APP token）

## 2. 部署拓扑

```
脚本/轻客户端 (APP token) ──┐
助手 (Kelivo, ADMIN token) ─┤  HTTPS https://archive.example.com/archive/...
                          ▼
              cloudflared 隧道 (tunnel-id)
                ingress: archive.example.com → 127.0.0.1:3012
                          ▼
        archive-mcp (server.js, node 22, systemd: archive-mcp.service, User=agent)
              │
              ├── SQLite  /home/agent/archive-mcp/data/archive.sqlite
              │            ├── 业务表（承接 D1 ARCHIVE_DB）
              │            ├── vec_chunks / vec_meta        （chunk + 记忆向量）
              │            └── vec_events / vec_events_meta （事件向量 whole+frag）
              ├── 原文    /home/agent/archive-mcp/data/raw/{uuid}.json
              └── HTTP → 127.0.0.1:3013 embed-server（Qwen3-Embedding-0.6B）

夜间侧车：`agent-night.timer`（每日 04:04）→ `night.js` 以 ADMIN 通道调反思工具（出题/判定/改名），
收尾 `POST /archive/night-report` 写晨报；`GET /archive/night` 的 enabled=false 时不唤醒。
```

依赖 `mcp-base.js`（已内置在本仓库）：MCP Streamable HTTP 传输 + 双 token 鉴权 + REST 路由框架。

## 3. 数据规模（2026-09-16 实测）

| 实体 | 数量 | 说明 |
|---|---|---|
| events | 307 | 事件 |
| event_edges | 1931 | overlap 1222 + thematic 650 + causal 59（夜间反思持续增长） |
| event_communities | 23 | Louvain 故事群 |
| profile | 8 | 档案卡（助手的系统提示词分卡） |
| night_reports | 1 | 晨报存档（每晚累积） |
| 向量 vec_events_meta | 1600+ | whole + frag |

（完整版另有 conversations/blocks/block_chunks/memory_notes 等原始层表与 3.4 万条 chunk 向量，本版未暴露。）

## 4. 存储层

### 4.1 SQLite（node:sqlite DatabaseSync, WAL, allowExtension:true）

| 表 | 关键列 | 用途 |
|---|---|---|
| conversations | uuid PK, name, created_at, message_count, block_count, note | 窗口元数据 |
| blocks | id PK, conv_uuid, sender, type, text_content, tool_name/input, starred | 内容块 |
| block_chunks | id PK, block_id, chunk_idx, char_start/end, text_content, UNIQUE(block_id,chunk_idx) | 分块 |
| memory_notes | id TEXT PK(num[-sub]), num, sub, title, body | 归纳叙事 |
| emo_axes | axis, pole(pos/neg), version, anchor_text, active | 情绪轴锚点 |
| edges | (block_a,block_b) PK, weight | 块共现表（2026-09-05 起停止写入，保留结构备将来） |
| events | id PK, title, summary, time_span_start/end, themes, source_block_ids, importance, event_type, vector_dirty | 事件 |
| event_edges | (event_a,event_b,edge_type) PK, detail(JSON), confidence | 事件边，见 §5.1 |
| event_communities | id PK, name, member_ids, core_event_id, internal_edges, renamed | 故事群（renamed=1 的名字在 rebuild 时按成员 Jaccard≥0.8 继承） |
| causal_reviews | id PK, event_id, candidate_id, verdict(link/reject/done/defer), reason, decided_by, defer_until | 反思判定留痕 |
| profile | id(序号,1..n 连续), title PK, content, updated_at | 档案卡：助手的自我叙事+关系认知，按 id 顺序拼起即下一轮 system prompt；整卡覆盖写，支持改名/移位（其余卡自动重排） |
| night_switch | id=1, enabled, updated_at | 助手的夜间唤醒开关（agent-night 启动前查询，关着不唤醒） |
| night_reports | date PK, verdicts_total/link/reject/defer/done, renames, queue_causes/effects, elapsed_min, detail_json | 晨报存档（dream_words/逐条 verdict 明细/改名/notes/timeline 在 detail_json，按日期整份覆盖） |

### 4.2 向量表（sqlite-vec 0.1.6）

```
vec_chunks:      vec0(embedding float[1024])
vec_meta:        rowid PK, kind(chunk/memory), ref_id, text(300字预览)
vec_events:      vec0(embedding float[1024])
vec_events_meta: rowid PK, event_id, kind(whole/frag), frag_idx, frag_text
```

**rowid 约定**（三组互不踩）：
- chunk → `block_chunks.id`（1..34606）
- 记忆（backfill / MCP 导入）→ `1000000 + idx`
- 记忆（REST POST /memories 即时 embed）→ `2000000 + num*100 + sub`
- 事件 → `eventId*1000`（whole），`eventId*1000 + fragIdx + 1`（frag）；删除按 `BETWEEN eventId*1000 .. +999` 清段

### 4.3 sqlite-vec 要点（踩过的坑，仍然有效）

1. `load(db)` 后必须同一连接操作
2. `WHERE embedding MATCH ? AND k = ?` 不能与 LIMIT 混用，取 k+余量后自行裁
3. rowid 绑定用 `BigInt(...)`，回读是 number
4. 绑定向量：`new Uint8Array(new Float32Array(vec).buffer)`（float32 小端）
5. vec0 对已有 rowid 不支持 INSERT OR REPLACE → `vecPut()` 先删后插，调用方事务内自动并入
6. 加载失败不致命：`vecReady=false` → recall 降级字面搜索

## 5. 事件网络层

### 5.1 图模型

- **节点**：events（id, title, summary, time_span, themes, importance, event_type ∈ moment/period/milestone/ritual）
- **边**：event_edges，无向存储（event_a < event_b），三类型：
  - `causal`（权重 3.0）——因果关联，**只由人/AI 反思产出**（手动边 `detail.manual=true`，可带 `dir` a→b/b→a、`name`、`reason`）
  - `thematic`（1.5）——语义血缘，rebuild 时 qwen 余弦 ≥ 阈值自动生成，detail 存 `semantic_score` + 命中的 `frag_text`（120 字证据）
  - `overlap`（1.0）——时间跨度重叠，rebuild 自动生成
- **社区**：event_communities，Louvain 聚类结果；孤立/有边未聚类事件在 viz 中归入「孤立」「未聚类」虚拟组
- **向量**：每事件 whole（title+summary 整体）+ frag（summary 按自然段/句子切，≥8 字）双向量；thematic 边阈值 `config.thematicThreshold = 0.748`（qwen 余弦尺度，用户定 ≈2.4 条语义边/事件）

### 5.2 rebuild 管线（rebuildEventNetwork）

```
脏事件(vector_dirty=1) 增量 embed → vec_events
  → overlap: 时间跨度两两对比 O(n²)
  → thematic: 全量向量读回内存, 暴力余弦, ≥0.748 建边（批量失败自动降级逐条重试隔离）
  → Louvain 重新聚类（renamed 社区按 Jaccard≥0.8 继承名字）
手动边(detail LIKE '%manual%')全程保留；进度经 NDJSON 流式端点输出
```

- `_rebuilding` 标志防并发；耗时集中在 embed（本地 CPU，首次全量数十分钟；日常增量秒级）
- **崩溃安全**：清空前整表备份到 `event_edges_backup`，成功删备份、失败保留 → `archive_restore_network` 回滚

### 5.3 暴露面：三通道 + 网关

| 通道 | 鉴权 | 暴露深度 |
|---|---|---|
| **MCP**（`/archive/mcp`） | 仅 ADMIN（助手） | 最深：事件 CRUD、图遍历、反思循环全量、重建 |
| **REST**（`/archive/*`） | APP 或 ADMIN（脚本/网关） | 事件 CRUD、1 跳 graph、network-stats、probe、rebuild(+stream)；反思三工具暂无 REST |
| **viz**（`/archive/viz`） | cookie（token 换签） | 用户的人肉视图：`/data` 全图快照（echarts 形状）、手动边、重聚类。**viz 是用户看的，改动不经此通道** |
| memory-gateway connector | 内网 appToken | 仅事件列表/详情/stats——**边、社区、图未过网关**（已知缺口） |

### 5.4 AI 侧图工具（2026-09-14 增强）

设计原则：图算法全是内存级（290 节点），瓶颈是 token 与往返次数——**一次调用给足结构，每个返回带「下一步」note**。

| 工具 | 能力 |
|---|---|
| `archive_event_graph` | 单事件 1 跳邻居，按边类型分组 |
| `archive_event_subgraph` | **新增**。seed(s) 出发按边权 Dijkstra 最大化在 radius(1-3) 跳内扩展，max_nodes(5-40) 剪枝；节点带故事群归属，边带 dir/reason/semantic_score/frag_text；扁平边表形状（LLM 读边表比读树稳） |
| `archive_network_stats` | **新增**。全局结构：度数 top10 枢纽 + **桥边**（跨故事群的边，causal 优先，上限 100）+ **割点**（Tarjan，拿掉即断网的枢纽，上限 50） |
| `archive_causal_chain` | 沿有向因果边贪心走主线（每层只展开最高权重边），branches 记录被压掉的分支，dead end 标记 |
| `archive_associate` ✦✦ | **联想检索**（2026-09-16，参照 HippoRAG pattern completion）：context 语义命中（或直传 seeds）→ 事件网 Personalized PageRank 扩散（teleport 0.15，causal 3.0 主导方向）→ top-k + via（被谁用什么关系拉上来）。找关联而非相似 |
| `archive_event_path` ✦✦ | **A→B 路径查询**（2026-09-16，RRP 式评分）：有界枚举简单路径（≤5 跳，30 万扩展预算），reliability = Σ边权（causal 顺方向×1.2/逆×0.8）− 每段 0.3 长度惩罚，降序。实测 290 节点 84ms/676 条 |

### 5.5 反思循环（2026-09-14 增强）

```
archive_causal_probe   出题：四层候选（hop1 已有边提升 / hop2 两跳链 X-W-Y / hop3 同社区未连 / **hop4 remote✦✦ 跨群·无边·时序走廊**）
                       评分 = 边权 + 0.8·时间邻近 + 0.5·同社区；hop1 候选带 evidence（语义分数+frag 原文）
                       hop4 remote：跨故事群·无任何边·间隔 7-90 天的远端 pair（InfraNodus 结构洞 + LLM pairwise 判读范式），
                       排序 = 结构位(群内度数前3) + 重要度 + 远而不隔(共同邻居)；判定复用 review，人在环不自动建边
archive_causal_review  判定：
  present   端菜——focus 队列: causes=无因事件(默认) / effects=无果死胡同 / auto=并集；
            importance↓ degree↑ 排序取前 3，每事件 1+1 候选(空侧补 2)；detail=true 摘要 120→400 字
  verdicts  批量判定(≤50)——单事务，逐项校验，异常整体回滚
  单条      link / reject / done / defer(默认14天)，与旧参数完全兼容
  link 成功 → 建 causal manual 边 + echo 回声（新边两侧一跳因果邻域, role=cause_of/effect_of）
判过的 pair（reject/link）不再出题；done 事件退出队列；defer 到期重新入队
```

单条判定逻辑在 `applyOneVerdict()`（单条/批量共用）；方向默认按时间先后推断（早→晚），`direction` 可覆盖。

### 5.6 事件检索（混合，供 archive_search_events）

语义路（query → vec_events KNN，frag 命中归属整事件，whole 兜底）+ 关键词路（BM25 over 标题×2+正文，中文二元组切词）→ RRF（K=60）融合。embed-server 不可用时语义路自动降级、关键词路不受影响（`semantic_degraded: true`）。

## 6. MCP 工具（27 个，仅 ADMIN · 事件网络版）

| 组 | 工具 |
|---|---|
| 总览（1） | archive_overview（事件/故事群/边计数 + 最近事件） |
| 事件（6） | archive_list_events / search_events(混合RRF) / read_event / create_event / update_event / delete_event |
| 事件网络（9） | archive_event_link / event_unlink / event_graph / **event_subgraph✦** / **network_stats✦** / **associate✦✦** / **event_path✦✦** / rebuild_network / restore_network |
| 社区（3） | archive_list_communities / community_rename / community_delete |
| 反思（3） | archive_causal_probe(hop4 remote✦✦) / causal_review(focus·verdicts·echo✦) / causal_chain |
| 夜间（3） | archive_night_status（开关+最近晨报） / night_toggle（唤醒开关） / night_reports（最近 N 份晨报+梦话，默认3）✦ |
| 档案卡（2） | archive_profile_read（分卡 / mode=assembled 拼 id 顺序全文） / profile_edit（id 或 title 定位；content 整卡覆盖 / new_title 改名 / new_id 移位）✦ |

✦ = 本地新增/增强；事件网络的 associate / event_path / probe-hop4 为 2026-09-16 联想层。窗口/原文、块召回、摘要记忆、情绪轴四组工具已在事件网络版中移除

## 7. REST API（basePath = `/archive`；除标注外 APP/ADMIN 平权）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | /stats | 事件/边/故事群计数 |
| GET | /events、POST /events/create、GET/PATCH/DELETE /events/:id、GET /events/:id/graph | 事件 CRUD |
| POST | /events/probe | 向量近邻探针（REST 侧） |
| POST | /associate、GET /events/:from/path/:to | 联想（PPR）/ 路径查询 ✦✦ |
| GET | /events/network-stats | 度数明细全量 + 桥边/割点 ✦ |
| POST | /events/rebuild-network（同步 / /stream NDJSON）、/events/restore-network | 网络重建 |
| POST/DELETE | /events/link | 手动边 |
| GET | /communities、PATCH/DELETE /communities/:id | 社区 |
| GET | /profile（?mode=assembled=按 id 拼接全文）、PUT/PATCH /profile/:title或:id | 人格画像：读 / 建档改内容 / 改名(new_title) / 移位(new_id) |
| GET | /night（开关+最近晨报）、POST /night {enabled}；GET /night-reports?limit、/night-reports/:date；POST /night-report | 夜间唤醒与晨报（开关/查询 APP+ADMIN，上报仅 ADMIN） |
| viz（cookie） | GET /viz、GET /data、POST/DELETE /link、POST /communities/rebuild、GET /echarts.min.js | 用户的页面（echarts.min.js 为无鉴权静态文件，唯一例外） |

## 8. 鉴权模型

| 通道 | token | 说明 |
|---|---|---|
| MCP（/archive/mcp） | 仅 ADMIN | 助手 |
| REST（/archive/*） | APP 或 ADMIN | 脚本/memory-gateway；read_window 原文、search、raw 仅 ADMIN |
| viz | cookie `viz_token`（?token= 换签，HttpOnly 30天） | 用户 |

token 落服务器 `/home/agent/.secrets/`；网关侧 connector 内部持 appToken，不透传客户端 token。

## 9. 部署与运维

```
/home/agent/archive-mcp/
  server.js            # 服务主体（~3000 行；依赖 ./mcp-base.js）
  config.json          # port=3012 / appToken / adminToken / dataDir / embedPort=3013 / thematicThreshold=0.748
  louvain.js backfill.js import-memories.js echarts.min.js viz.html
  data/archive.sqlite  # SQLite + vec0（WAL）
  data/raw/*.json      # 原文
  server.js.bak-*      # 每次改动的远端备份（日期-标签）

systemctl restart archive-mcp    # journalctl -u archive-mcp -f
systemctl restart embed-server   # curl 127.0.0.1:3013/healthz；模型重载≈25s
systemctl list-timers | grep agent-night   # 夜间唤醒定时器（04:04）；开关存 night_switch 表
```

**部署流程（2026-09-14 起的标准动作）**：远端 `cp server.js server.js.bak-<date>-<tag>` → scp 覆盖 → `node --check` → `sudo systemctl restart archive-mcp` → `curl 127.0.0.1:3012/archive/stats` + 冒烟端点验证。

**代码地图（函数级）**：embedTexts→embed-server；vecPut/readVecRow（向量读写）；rebuildEventNetwork / runCommunityDetection / restoreEventNetwork（网络生成）；buildSubgraph / dbNetworkStructure / dbEventCommunityMap（图查询 ✦）；eventGraph / pprRun / archiveAssociate / findEventPaths / archiveEventPath / remoteCausalCandidates（联想层 ✦✦）；loadProbeGraph / probeCandidates / applyOneVerdict / chainEcho（反思循环 ✦）；dbListProfile / dbUpsertProfile（档案卡）；vizData（viz 快照）；MCP_TOOLS / callTool / handleRest（三入口共用函数层）。

## 10. 已知限制与后续

- **hop4 remote 二期**：结构摘要（助手给每卡写"去掉表面主题的关系骨架"）可增强远端召回——当前纯结构信号（时序走廊+结构位+重要度+共同邻居）先跑基线；队列 3.2 万对，靠判定/走廊收缩
- **PPR 权重仍是类型常数**（causal 3.0/thematic 1.5/overlap 1.0）：semantic_score/importance/重叠时长未进权重——有意保持粗先验（阈值线附近 qwen 噪声大），等联想层有使用基线再决定是否细化
- **反思评分未纳入** source_block_ids 共现、themes 交集信号（纯内存可加）
- **runCommunityDetection 未 MCP 化**：反思攒 causal 边后社区结构漂移，只有 viz REST 能单独重聚类，助手够不着
- **反思三工具无 REST**：causal_probe（含 remote）/review/chain 仅 MCP；agent-night 若要跑 remote 出题需接 MCP 或加 REST
- **安全备注**：rebuild-network（含 stream）APP token 可触发（CPU 数十分钟级）；viz token 走 URL query 会进隧道访问日志；手动边直连 link 无 decided_by 留痕（review 判定才有）
- memory-gateway 只到事件时间线，边/社区/图快照未过网关
- 旧账：286 条 D1 脏 chunk（U+FFFD/lone surrogate）可重 embed 修复，0.8% 噪声优先级低

## 11. 事故档案

| 日期 | 事故 | 根因 | 修复 |
|---|---|---|---|
| 08-21 | TextEncodeInput @回填 | D1 脏文本 lone surrogate → Rust tokenizer 拒收 | embed-server 入口 `encode("utf-8","ignore")` 清洗 |
| 08-21 | recall 乱码 score -0.06 | Windows curl 中文 GBK 编码（库没坏） | node UTF-8 请求验证 |
| 08-21 | 回填差 8 条 | 重启瞬间 embed-server 加载中 | 幂等重跑补漏 |
| 08-21 | embed-server 重启后不自启 | systemd is-enabled=disabled | `systemctl enable` |
| 08-21 | 回填进程重启后丢失 | nohup 不是 systemd | 幂等续跑（教训：长任务要么服务化要么可重跑） |
| 09-09 | viz 时间线改版 | — | viz.html.bak-20260909-timeline 留档 |

## 12. 变更记录

| 日期 | 变更 |
|---|---|
| 08-22 | 阶段 2 全功能版上线（29 工具，脱离云端独立运行） |
| 09-05 ~ 09-07 | optout / q 参数 / 混合检索（语义+BM25 → RRF）等迭代（见 server.js.bak-2026xxxx 标签） |
| 09-08 | 写路径合并——读写只走主家 |
| 09-11 | admin 鉴权细化 |
| 09-14 | AI 侧图工具：archive_event_subgraph / archive_network_stats（桥边·割点）；profile 档案卡上线（MCP） |
| 09-15 | profile id 化（改名/移位自动重排）+ REST 通道（档案卡读取与编辑） |
| 09-16 | 联想层 archive_associate（PPR）/ archive_event_path（RRP 评分）；probe hop4 remote 远端因果候选；助手侧 night/timeline/dreamwords 同期上线；archive_night_reports 晨报读取 |
