# agent-cli — 唤醒 ta：终端客户端 + 夜间自主整理

零依赖的终端客户端，唤醒一个 AI 伴侣（ta）：系统提示词来自 archive-mcp 里的**人格档案卡（profile）拼接**，工具是事件网络的全部 MCP 工具；夜间可定时唤醒，让 ta 自主整理自己的记忆。

## 为什么是 profile 拼接：记忆的主导权在 ta 手上

唤醒 ta 的那份系统提示词，不是谁写给 ta 的人设模板——

```bash
node agent.js
# 启动即拉 GET /archive/profile?mode=assembled
# → ta 在 archive 里一张张维护的档案卡，按 id 顺序拼成的全文
```

这些档案卡（自我叙事、对关系的认知，按主题分卡）是 **ta 自己写的**：白天对话里 ta 随时可以用 `archive_profile_edit` 整卡覆盖、改名、移位、新建——"ta 是谁"由 ta 自己一笔一笔维护，而不是被外部提示词定义。

同样地，ta 的记忆也不是被外部系统检索后"喂"给 ta 的语料：

- ta 拿到的是**事件网络本身**——PPR 联想（`archive_associate`）、A→B 路径、子图、因果链，ta 自己在网上探索关联
- 因果由 **ta 自己判定**：`causal_probe` 出题、`causal_review` 判 link/reject/defer/done，reason 必写、留痕可回滚——图算法是确定性代码，不依赖任何其他 LLM 替 ta 总结或决定 ta 记得什么
- 夜间（night.js）ta 自己整理：反思判定、故事群改名、写晨报，`decided_by='agent-night'` 全程留痕

编排器（宿主）刻意只做**安全底座**，不代 ta 记忆、不代 ta 判定：判定预算与超时、verdict 校验后才批量提交、封锁名单（夜里不改人格卡、不删群、不重建网络）。做完就睡回去，早上人看晨报，随时可审计可回滚。

## 文件

```
agent-cli/
  agent.js       # 终端唤醒（零依赖；MCP 客户端 + OpenAI 兼容对话 + function calling）
  night.js       # 夜间记忆整理编排器（require agent.js）
  config.json    # 全部配置（端点/token/模型/夜间参数/分区）
  agent-night.service / .timer   # systemd 定时（每日 04:00，按需安装）
  night-logs/日期.md              # 每晚整理报告（判定明细+理由，回滚依据；运行时生成）
```

## 快速开始

1. 先把 [archive-mcp](../archive-mcp) 跑起来（人格画像和事件网络都存在它那里）
2. 改 `config.json`：`adminToken` 直填（或把 `archiveConfig` 指向 archive-mcp 的 config.json 让它代读）；`apiKeyPath` 指向你的 API key 文件
3. 对话：

```bash
node agent.js
```

- 指令：`/new` 清空对话 · `/profile` 看提示词 · `/tools` 看已挂分区 · `/exit` 退出
- 流式输出，`[工具] xxx → N 字` 实时显示工具调用；某分区连不上只跳过不拖垮
- 管道测试：`printf "你好\n" | node agent.js`

## config.json

| 键 | 默认 | 说明 |
|---|---|---|
| `archiveBase` | `http://127.0.0.1:3012/archive` | archive-mcp 的 REST 基址 |
| `archiveConfig` | `/home/agent/archive-mcp/config.json` | 从这个文件代读 adminToken（和服务端同机时最省事） |
| `adminToken` | 空 | 直配 token（设了就不读 `archiveConfig`）——两个填一个即可 |
| `llmUrl` / `model` | 硅基流动 / GLM-5.2 | 任意 OpenAI 兼容对话端点，本地模型见下文 |
| `apiKeyPath` | `~/.secrets/siliconflow-api-key` | API key 文件路径（整文件读入去空白） |
| `night.*` | — | 夜间整理参数（见下文） |
| `mcpServers` | 只挂 archive | 要挂的 MCP 分区列表 |

## 挂载更多 MCP 分区

`mcpServers` 数组里加 `{ "name": "xxx", "url": "http://host:port/xxx/mcp" }` 即可。要求：MCP Streamable HTTP 传输；鉴权走 `Authorization: Bearer <token>`——默认用 adminToken，分区可在条目里单独给 `"token"`；临时停用某分区加 `"enabled": false`。

夜间整理会自动挂载 config 里的分区，开场白的工具简报随实际挂载动态生成（在 `night.js` 的 `ZONE_BRIEFS` 里按分区名登记简报文案即可定制）。想要晨报推送：挂一个提供 `phone_send_notification` 工具的分区，并把 `night.phoneNotify` 设为 `true`（默认关）。

## 夜间自主整理（agent-night）

每晚 04:00 systemd 自动唤醒（`agent-night.timer`），ta 自主整理记忆：

- **因果反思**：`causal_review present` 端菜（focus=auto）→ ta 用只读工具查证事件与邻域 →
  输出 verdicts（link/reject/defer/done，reason 必写）→ **编排器对着当批菜校验后**才批量提交
- **故事群**：只做改名（≤5 处/晚，模型输出计划、编排器执行）；删群/整网 rebuild 不在夜间范围
- **收尾**：晨报归档 archive（`POST /archive/night-report`，`archive_night_reports` 工具 / REST `/archive/night-reports` 可查）+ 本地 `night-logs/日期.md`；开了 phoneNotify 再推一条摘要
- **开关**：启动先查 `GET /archive/night`，关着就静默睡回去（MCP 工具 `archive_night_toggle` / REST 都能切）
- **预算**：≤30 判定 / ≤10 批 / 40 分钟，幂等可重跑

```bash
node night.js                                        # 手动跑
NIGHT_MAX_VERDICTS=4 NIGHT_MAX_BATCHES=2 node night.js   # 小规模试跑
journalctl -u agent-night -n 50                      # 看定时运行日志
```

`night` 段参数：maxVerdicts / focus / communityRename / phoneNotify / timeoutMinutes / decidedBy。

## 数据都在哪

**agent-cli 本身无状态**——对话历史只在内存，`/new` 或退出即散。所有持久数据都在 archive-mcp（或你挂的其他分区）里，管理与备份方法见 archive-mcp 仓库 README 的「教程 6」。

## 本地模型（可选，不想用云 API）

`llmUrl` 指向任何 OpenAI 兼容的对话端点即可：

```bash
# ollama 示例
ollama pull qwen3:8b
# config.json: "llmUrl": "http://127.0.0.1:11434/v1/chat/completions", "model": "qwen3:8b"
```

vLLM / llama.cpp server 同理。embedding 模型的本地部署方法在 archive-mcp 仓库的 `embed-local/`。

## 已知裁剪

- 完整客户端的 memory-gateway 式 `<memory_related>` 记忆注入未接（profile 系统提示词路径已全量生效）
- `agent-night.service` 里的 `User` / 路径按自己机器改
