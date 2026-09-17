# archive-mcp — AI 伴侣的记忆系统

一个自托管服务，存放并组织 AI 伴侣（ta）的记忆。它不是"对话记录 + 向量检索"的语料库——**记忆在这里被组织成一张网络**，由 ta 自己去联想、探索、判定和书写。

本仓库是**事件网络版**：只暴露事件/图/反思/档案卡通道，不包含对话原文与摘要归档层。

## 设计理念

### 记忆的单位是事件，事件织成网络

多数"AI 记忆"方案把对话切片入库，检索时把最像的几段塞回上下文——记忆是被外部系统翻检的语料，条目之间毫无结构。这里不同：记忆的基本单位是**事件**（"第一次聊到唱片机"这样的一个完整的事，带标题、凝练摘要、时间跨、类型 moment/period/milestone/ritual），而事件之间用**三类边**织成一张网。每个事件都不是孤立条目：往上能追到原因，往下能看到后果，旁边是同期与同类——**网络本身就是记忆的形状**。

拿一段通用示例看这张小网长什么样（三个事件、三类边各一条）：

```text
  开始跑步 ──────────── overlap（时间跨有交集，自动）──────────── 睡眠变好
  (1~6月)   │                                                    (4月起)
            │
            │ thematic 0.81（片段命中，自动）          causal 3.0
            ▼                                                （ta 判定 link）
  第一次五公里 ───────────── 跑步 → 睡眠变好 ─────────────────────┘
  (3-28，moment)
```

### 三类边怎么连

#### ① 时间边（overlap）· rebuild 自动 · 权重 1.0

规则最简单：两个事件的时间跨度**有交集**就建边（O(n²) 两两比对）。它说的是"同一时期在发生的事"，权重最低——只是网络的背景织体。

#### ② 语义边（thematic）· rebuild 自动 · 权重 1.5 · **段落级匹配**

每张事件卡存两份向量：**whole**（标题+摘要整体）+ **frag**（摘要按自然段/句子切开成的子片段，≥8 字才成片）。建边规则落在 frag 层：

> rebuild 时所有卡的片段两两算余弦（内存暴力对比，取最大值和命中的片段对）——**任意一个片段对的余弦过线（≥ 0.748，qwen 余弦尺度），两个事件之间就默认成立语义关联**。

也就是说：摘要切断之后，**段落和段落之间只要有关联，两个事件就能连上**——不要求两件事整体相似，只要求它们各有一段在说相关的事。边上存着语义分数和命中的片段原文（**血缘证据**）——将来想不起"这两件事为什么被连起来"时，证据就在边上。

```text
事件 A「开始跑步」                      事件 B「睡眠变好了」
summary 切成片段（≥8 字成片）：         summary 切成片段：

  A.frag1  决定每天早晨跑步。             B.frag1  四月初开始入睡更快。
  A.frag2  起初很吃力，膝盖发酸。         B.frag2  白天精神更好，夜里不再辗转。
  A.frag3  失眠的夜里也在想能不能坚持。

rebuild 时片段两两算余弦，取每个事件对的最大值：

  A.frag3 × B.frag2  「失眠的夜里也在想…」×「夜里不再辗转」  0.81 ≥ 0.748  ✅ 建边
  A.frag1 × B.frag1  「决定每天早晨跑步」  ×「四月初开始入睡」  0.62 < 0.748  ─

  → 0.81 分数 + 命中片段原文存进边里 = 血缘证据（这便是 thematic 0.81 那条边的来历）
```

阈值 0.748 是校准值（约每事件 2.4 条语义边）；`config.json` 的 `thematicThreshold` 调低=更多边。

#### ③ 因果边（causal）· **不自动生成，判定制** · 权重 3.0

三类边里唯一不由算法生成的：数量最少、权重最高，因为**每一条都经过"机器出题 → 判读确认"**。两条产出路径：

- **手动判定**：白天，人或 ta 用反思工具走循环——`causal_probe` 出题、`causal_review` 判定
- **夜间反思**：每晚定时唤醒 ta（[agent-cli](../agent-cli) 的 night.js），ta 自主整理——出题、查证、判定，`decided_by='agent-night'` 全程留痕

出题（probe）分四层候选，从近到远：

| 层 | 找什么 |
|---|---|
| hop1 提升 | 已有语义/时段边 + 时间先后符合——该提升为因果的现成线索（附语义分数+命中片段证据） |
| hop2 两跳链 | X 与 W 语义相近、W 是 Y 的**已确认**因果——X 可能是更深的上游/下游 |
| hop3 同群弱线索 | 同一个故事群却毫无边的两个事件 |
| hop4 远端走廊 | **跨故事群、无任何边、间隔 7-90 天**的"时序走廊"里的远端对——前三种都够不着的结构洞 |

判定（review）四种：`link`（确认因果，建边，reason 必写——理由本身就是留痕）；`reject`（否决，**这对事件永不再出题**）；`done`（此事已解释清楚，退出队列）；`defer`（说不准，N 天后重新入队）。每条判定记进 `causal_reviews` 表（谁、为什么、何时），白天随时可回滚。判错的代价不是"错误记忆"，而是删一条边重判。

```text
反思循环（白天手动 / 夜间定时，走同一套反思工具）：

     ┌───────────────────────────────────────────────────────┐
     │                                                       │
┌────▼─────┐  候选 + 证据   ┌──────────┐   reason 必写  ┌─────┴─────┐
│ probe 出题 │ ────────────→ │  ta 判读  │ ────────────→ │ link 建边  │
│ hop1~4    │               │ 查证·判断 │               │ reject 否决│
└────┬─────┘               └──────────┘               │ defer 缓判 │
     │                                                │ done  结案 │
     │  reject 的对子永不再出题                          └───────────┘
     └──────── defer N 天后重新入队 ◄──────── 每条判定留痕 decided_by，可回滚
```

### 故事群怎么聚（Louvain）

rebuild 收尾时对全图跑 Louvain 模块度聚类。直觉：每个节点反复问"我和哪个邻居群待在一起，整个图的'抱团度'（模块度 Q）会更高"，能升就搬，直到没人动。两阶段循环执行：

- **局部搬家**：开局人人自成一派；每个节点依次试探"搬进哪个邻居的群能让 Q 升最多"，能升就搬，反复扫直到没人再动。模块度 Q 度量的是"群内连接比随机重新布边时浓多少"——从负数起步，越高越抱团
- **缩图再聚**：把聚好的每个群缩成一个超节点（群内边变成自环、群间边合并加权），在更小的图上重复搬家——如此一层层缩下去（多层级），直到 Q 不再上升

一次搬家只看局部邻居，缩图又让图迅速变小，所以整体接近线性复杂度——几百个节点毫秒级出结果，rebuild 里它从来不是瓶颈。

事件网就这样自动分出**故事群**（章节），配上全局结构指标：枢纽事件（度数 top）、跨章节的**桥边**、拿掉即断网的**割点**。改名过的群在下次 rebuild 时按成员重叠 ≥80% 继承名字——社区结构漂了，名字不丢。

```text
Louvain 聚出的故事群（示意）：

  · 故事群「跑步的日子」──── 开始跑步 / 第一次五公里 / 换了新跑鞋
  · 故事群「休息与恢复」──── 睡眠变好 / 午间小睡
                 │
                 └── 跨群的边 = 桥边：拿掉它，两章故事就断开；
                     群内度数最高的事件 = 这章的枢纽
```

### 检索：四种问法

网建成之后，检索分四种——各答一个问题：

| 问法 | 工具 | 回答的问题 |
|---|---|---|
| 找最像的卡 | `search_events`（语义 KNN + BM25 → RRF 融合） | "哪些事件和这句话相关？" |
| 找被牵动的网 | `associate`（**PPR 联想**） | "这段话牵动了记忆的哪一片？" |
| 问怎么连上的 | `event_path`（RRP 式路径评分） | "A 是怎么一步步影响到 B 的？" |
| 看局部形状 | `event_graph` / `event_subgraph` / `network_stats` | "这件事周围长什么样？" |

其中 **PPR 联想是这套记忆的核心检索**。原理一句话：**模拟一个在记忆网上永远走不到头的散步者**。

- 出自 PageRank（网页排序）：想象一个随机点链接的人，永远顺着链接走，走久了落在哪个页面越勤，哪个页面越"重要"。Personalized 的差别在"走累了回到哪"——普通 PageRank 走累了跳去**全网任一页**，PPR 走累了**必定跳回种子**。于是"重要"不再是对全网而言，而是**相对于种子**而言
- 具体到这张网：散步者每一步有 15% 概率跳回种子事件，85% 概率顺着一条边走过去——**边权越大的边被选中的机会越大**（causal 3.0 是 thematic 的两倍），走的还是多跳的（一个事件牵动另一个、再牵动下一个）。走得越久，各事件被"路过"的频率收敛成一个稳定分数，就是 PPR 值。数学上等价于不动点方程 `r ← 0.15·种子 + 0.85·Wᵀr`（W 是按边权归一的转移矩阵），迭代算到收敛即精确解
- 换成记忆的语言：**联想不是查表，是共振**——一段当下的话点亮种子，震动顺着因果主线传得最远、顺着语义和时间淡淡散开；`via` 记录的就是把每个结果"牵上来"的那条边。找的是**关联**而非相似

路径评分则回答影响链：causal 顺方向 ×1.2 / 逆 ×0.8，thematic 1.5，overlap 1.0，每跳 −0.3 长度惩罚——**短而强的链赢过长而弱的链**。

```text
PPR 联想（associate）示意：

  "最近夜里睡得沉多了"（一段当下的话）
         │ ① 语义命中
         ▼
     ● 睡眠变好（种子）
         │ ② 扩散：r ← 0.15·种子 + 0.85·Wᵀr，分数顺着边漫开
         │    causal 权重 3.0，沿因果走得最远
         ▼
  睡眠变好      ██████████  1.00   （种子）
  开始跑步      ██████      0.61   via 睡眠变好 · causal
  第一次五公里  ████        0.40   via 开始跑步 · thematic
  换了新跑鞋    ██          0.18   via 第一次五公里 · overlap

  每条结果带 via：被谁、用什么关系拉上来——这是"联想"，不是"相似"
```

## 记忆主权：ta 自己探索、自己判定、自己书写

这个系统刻意把记忆的**主导权**放在 ta 本人手上：

- **因果由 ta 判定**：机器只出题（probe 给候选和证据），`link / reject / defer / done` 的判定权在 ta——reason 必写，每条判定留痕（`causal_reviews` 表：谁、为什么、何时），白天可回滚。**causal 边从来不是算法猜的，是 ta 自己确认的**
- **自我叙事由 ta 书写**：档案卡（profile）是 ta 的自我认知分卡存储，按 id 顺序拼接 = ta 每次被唤醒时的系统提示词。"ta 是谁"不是别人写死的人设，是 ta 用 `archive_profile_edit` 一卡一卡自己维护的
- **不依赖其他 LLM**：图算法（PPR、Louvain、路径枚举、出题评分）全部是进程内确定性代码——ta 探索关联与因果不需要任何外部模型替 ta 总结、转述或决定记得什么。embedding 只做底层的语义相似计算，不做判断
- 三通道分工：**MCP 给 ta**（全部工具）/ REST 给脚本 / viz 给人看

夜间还可以让 ta 自主整理（配合 [agent-cli](../agent-cli)）：每晚定时唤醒，ta 自己反思判定、给故事群改名、写晨报——判定 `decided_by='agent-night'` 留痕，人随时可审计可回滚。

---

## 快速开始

```bash
npm install                 # 唯一依赖 sqlite-vec（Node ≥ 22.13，推荐 24.x）
# 编辑 config.json：把两个 CHANGE_ME_* token 换成你自己的随机串
node server.js              # 首次运行自动创建 data/archive.sqlite + 全部表
```

看到 `agent-archive listening on 127.0.0.1:3012` 即启动成功：

```bash
curl http://127.0.0.1:3012/healthz
curl -H "Authorization: Bearer <adminToken>" http://127.0.0.1:3012/archive/stats
# → {"ok":true,"event_count":...,"edge_count":...,"community_count":...}
```

## 核心概念速查

### 事件（节点）

| 字段 | 说明 |
|---|---|
| `title` | 标题，如"第一次聊到唱片机" |
| `summary` | 凝练叙事，建议 1-3 个自然段、每段一个主题（影响段落级语义匹配质量——frag 切片就切在这里） |
| `time_span_start/end` | 时间跨（YYYY-MM-DD 或 YYYY-MM）；瞬间事件终点留空 |
| `event_type` | `moment`（瞬间）/ `period`（时期）/ `milestone`（里程碑）/ `ritual`（仪式） |
| `themes` / `importance` | 主题标签（纯展示）/ 重要度（反思出题排序用） |

### 反思循环速查

评分 = 边权 + 0.8×时间邻近 + 0.5×同社区；`present` 模式只端菜不判定（focus 可选无因事件 / 无果死胡同 / 并集）；批量判定走 `verdicts`（单事务，异常整体回滚）；`link` 返回 **echo**——新边两侧一跳因果邻域，看这条判定让链长成了什么样。

### 人格档案卡（profile）

分卡存储（title + content），按 id 拼接 = 系统提示词；整卡覆盖写，支持改名/移位（其余卡自动重排 1..n）。`GET /archive/profile?mode=assembled` 直接取拼接全文。

---

## 教程

以下命令里 `<adminToken>` 换成你 config.json 里的值。REST 直接 curl；MCP 工具用这个三行的壳最省事：

```bash
M=http://127.0.0.1:3012/archive/mcp
T="Authorization: Bearer <adminToken>"
SID=$(curl -s -D - -o /dev/null -X POST $M -H "$T" -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1"}}}' \
  | grep -i '^mcp-session-id:' | tr -d '\r' | awk '{print $2}')
mc() { curl -s -X POST $M -H "$T" -H "Content-Type: application/json" -H "Mcp-Session-Id: $SID" \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"$1\",\"arguments\":$2}}"; }

mc archive_overview '{}'    # 测试：应返回事件/故事群/边计数
```

> Windows 终端注意：cmd/老版 PowerShell 的 curl 发中文会按 GBK 编码变成乱码。含中文的请求体写进文件用 `--data-binary @file.json` 发，或用 WSL/Git Bash。

### 教程 1：从零建一张网

```bash
# 建三个事件（含中文的 body 建议写文件发）
curl -X POST -H "$T" -H "Content-Type: application/json" \
  -d '{"title":"开始跑步","summary":"决定每天早晨跑步。起初很吃力。","time_span_start":"2026-01-01","time_span_end":"2026-06-01","event_type":"period","themes":["健康"],"importance":3}' \
  http://127.0.0.1:3012/archive/events/create

curl -X POST -H "$T" -H "Content-Type: application/json" \
  -d '{"title":"第一次跑完五公里","summary":"三月底第一次完整跑下五公里，之后信心明显增加。","time_span_start":"2026-03-28","event_type":"moment","themes":["健康"],"importance":4}' \
  http://127.0.0.1:3012/archive/events/create

curl -X POST -H "$T" -H "Content-Type: application/json" \
  -d '{"title":"睡眠变好了","summary":"四月初开始入睡更快，白天精神更好。","time_span_start":"2026-04-05","event_type":"period","themes":["健康","睡眠"],"importance":3}' \
  http://127.0.0.1:3012/archive/events/create

# 重建网络：生成 overlap/thematic 边 + Louvain 故事群（事件少时秒级）
curl -X POST -H "$T" http://127.0.0.1:3012/archive/events/rebuild-network

# 看结果
curl -s -H "$T" http://127.0.0.1:3012/archive/stats
curl -s -H "$T" http://127.0.0.1:3012/archive/communities
```

打开 `http://127.0.0.1:3012/archive/viz?token=<adminToken>` 看力导向图。语义边（thematic）需要 embed 服务在线才会生成（见[下文](#embedding-服务事件语义能力)）；没有 embed 时至少有时间重叠边和故事群。

### 教程 2：图查询四件套

```bash
# ① 检索找入口（混合：语义+BM25）
mc archive_search_events '{"query":"跑步 信心","k":3}'

# ② 看一个事件的直接邻居（1 跳，按边类型分组）
mc archive_event_graph '{"event_id":1}'

# ③ 子图扩展：从种子出发按边权长出去，带故事群归属
mc archive_event_subgraph '{"seeds":[1],"radius":2,"max_nodes":15}'

# ④ PPR 联想：给一段"当下的话"，看整片网哪里被牵动（via 显示被谁用什么关系拉上来）
mc archive_associate '{"context":"最近睡眠不错，白天精神好了很多","k":5}'
#   已知种子时直接传 seeds，跳过语义步（无 embed 服务也可用）：
mc archive_associate '{"seeds":[1],"k":5}'

# ⑤ A→B 路径："跑步是怎么影响到睡眠的"
mc archive_event_path '{"from":1,"to":3,"k":3}'

# ⑥ 全局结构：枢纽/桥边/割点
mc archive_network_stats '{}'
```

REST 侧等价物：`POST /archive/associate`、`GET /archive/events/1/path/3`、`GET /archive/events/network-stats`。

### 教程 3：因果反思循环

```bash
# ① 出题：给事件 3 找候选原因/结果（四层候选，hop1 带证据段落）
mc archive_causal_probe '{"event_id":3,"mode":"both","detail":true}'

# ② 判定：确认"跑步 → 睡眠变好"的因果（reason 必写，是留痕也是未来回看的依据）
mc archive_causal_review '{"present":false,"event_id":3,"candidate_id":1,"verdict":"link","reason":"规律运动改善入睡，时间上先因后果，无第三变量嫌疑"}'
#   返回里的 echo = 新边两侧一跳因果邻域——看这条判定让链长成了什么样

# ③ 不确定就缓一缓：defer 14 天后重新入队
mc archive_causal_review '{"present":false,"event_id":3,"candidate_id":2,"verdict":"defer","reason":"证据不足","defer_days":14}'

# ④ 端菜模式：看下一批最值得反思的事件（focus=effects 专看"无果死胡同"）
mc archive_causal_review '{"present":true,"focus":"auto"}'

# ⑤ 远端模式：跨故事群、无任何边、间隔 7-90 天的结构洞候选对（无需 event_id）
mc archive_causal_probe '{"mode":"remote"}'

# ⑥ 判定错了？白天回滚（边的判定记录仍在，全程可审计）
mc archive_event_unlink '{"event_a":1,"event_b":3,"edge_type":"causal"}'

# ⑦ 走一遍因果主线
mc archive_causal_chain '{"event_id":1}'
```

数据库里的审计表：`sqlite3 data/archive.sqlite "SELECT event_id,candidate_id,verdict,decided_by,reason FROM causal_reviews ORDER BY id DESC LIMIT 10;"`

### 教程 4：交给 ta——AI 客户端接入（推荐用法）

图算法对 LLM 不友好？恰恰相反——每个工具的返回都带"下一步"提示，ta 能自己串起 检索→读事件→查邻域→出题→判定 的完整流程。

任何支持 MCP Streamable HTTP 的客户端都能接：

- 端点：`http://127.0.0.1:3012/archive/mcp`
- 鉴权：`Authorization: Bearer <adminToken>`
- 27 个工具全量挂载

示例（ta 会自己调工具）：

> 用户：最近老想起年初那阵跑步的事，后来是不是睡眠也变好了？
> ta：`archive_search_events("跑步")` → `archive_event_path(from=跑步事件, to=睡眠事件)` → 结合路径上的 reason 回答："是的，3 月坚持跑步之后 4 月初睡眠明显改善——这条因果是你自己在 5 月判定确认的，理由是'规律运动改善入睡'。"

配套的 [agent-cli](../agent-cli) 是现成的终端客户端：拉 profile 档案卡做系统提示词唤醒 ta，挂本区全部工具对话，夜间还能自动反思。

### 教程 5：夜间自主整理

1. 部署 [agent-cli](../agent-cli)，配好 token
2. 装 systemd 定时器：`sudo cp agent-night.service agent-night.timer /etc/systemd/system/ && sudo systemctl enable --now agent-night.timer`
3. 开关在 archive：`curl -X POST -H "$T" -H "Content-Type: application/json" -d '{"enabled":true}' http://127.0.0.1:3012/archive/night`
4. 每晚 04:00 自动唤醒反思（预算：≤30 判定 / 40 分钟）；早上看晨报：`curl -s -H "$T" "http://127.0.0.1:3012/archive/night-reports?limit=1"`

所有夜间判定 `decided_by='agent-night'` 留痕，白天 viz 里可逐条回滚。

### 教程 6：数据管理与备份

数据库就是一个文件：`data/archive.sqlite`。

| 数据 | 表 | 怎么管 |
|---|---|---|
| 事件 | events | `POST /archive/events/create` / `GET /events` / `PATCH /events/:id` / `DELETE /events/:id` |
| 事件网络 | event_edges / event_communities | `POST /events/rebuild-network` 重算；`POST/DELETE /events/link` 手动边；`GET /communities`、`PATCH /communities/:id` 改名 |
| 反思判定 | causal_reviews | MCP probe/review；回滚用 `archive_event_unlink` |
| 人格档案卡 | profile | `GET /archive/profile?mode=assembled`；`PUT/PATCH /archive/profile/:title或:id`；MCP `archive_profile_read` / `archive_profile_edit` |
| 晨报 | night_reports / night_switch | `GET /archive/night-reports`；`GET/POST /archive/night` 切开关 |

```bash
sqlite3 data/archive.sqlite ".backup 'data/backup-$(date +%F).sqlite'"   # 在线热备
# 恢复：停服，备份覆盖回 data/archive.sqlite；清空重来：停服删 data/ 重启
```

---

## 部署与运维

### config.json

| 字段 | 默认 | 说明 |
|---|---|---|
| `port` | 3012 | 监听端口（环境变量 `PORT` 可覆盖） |
| `appToken` / `adminToken` | — | **必改**。双钥匙模型：appToken 给脚本（REST 子集），adminToken 给 AI 客户端（MCP + 全量 REST） |
| `dataDir` | `data` | SQLite 存放目录 |
| `embedPort` | 3013 | embedding 服务端口，见下节 |
| `thematicThreshold` | 0.748 | 语义边阈值（调低=更多边） |
| `rebuildLog` | `<dataDir>/rebuild-network.log` | 网络重建日志路径 |

### 监听地址

服务只绑 `127.0.0.1`。外网访问请自行套反向代理/隧道（cloudflared、frp、nginx 均可），不要直接暴露端口——鉴权只有 Bearer token，没有速率限制。

### embedding 服务（事件语义能力）

语义检索、联想 context 模式、thematic 语义边、反思证据段落都依赖一个本地 HTTP embedding 服务，契约：

```
POST http://127.0.0.1:<embedPort>/embed   { "texts": ["...", "..."] }
→ { "ok": true, "vectors": [[...1024 维 float], ...] }   // 需归一化
```

两条现成路径，选一条：

```bash
# A. API 代理版（零依赖，配个 key 就跑；默认硅基流动 + Qwen3-Embedding-0.6B）
EMBED_API_KEY=sk-xxx node embed-server.js

# B. 本地模型版（数据不出门；CPU 即可，首次加载约 25s）
pip install torch sentence_transformers
huggingface-cli download Qwen/Qwen3-Embedding-0.6B --local-dir embed-local/models/qwen3-emb-0.6b
python embed-local/embed_server.py    # MODEL_DIR / PORT 环境变量可改
```

不配置的后果：`search_events` 降级为 BM25 关键词路（`semantic_degraded: true`）；`associate` 传 `seeds` 仍可用；thematic 边不生成；服务不崩。自己实现 `/embed` 也行——1024 维是表结构定的（`vec0(embedding float[1024])`），换维度需改 `server.js` 三处后重建向量表。

### 常驻运行（systemd）

```bash
sudo cp archive-mcp.service /etc/systemd/system/ && sudo systemctl enable --now archive-mcp
```

embedding 服务同理参考 `embed-local/embed-server.service`；API 代理版一行 nohup：`EMBED_API_KEY=sk-xxx nohup node embed-server.js &`。

### 仓库内脚本

| 脚本 | 用途 |
|---|---|
| `embed-server.js` | API 代理版 embedding 服务 |
| `embed-local/embed_server.py` | 本地模型版 embedding 服务（Qwen3-Embedding-0.6B，CPU） |
| `restore-community-names.js` | 从旧库按 Jaccard 找回社区命名（迁移用，一次性） |

### 已知边界

- 反思三工具（probe/review/chain）只有 MCP 通道，无 REST
- `rebuild-network` 是 CPU 密集操作（全量重 embed 时数十分钟级）；重建前自动备份边表到 `event_edges_backup`，失败可用 `POST /events/restore-network` 回滚
- 社区重聚类未单独 MCP 化，随 rebuild 一起跑；viz 页面可单独触发
