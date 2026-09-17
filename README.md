# companion-memory — AI 伴侣的记忆系统

记忆不是被外部系统检索的语料库，而是一张由 ta 自己探索的事件网络。本仓库含两个组件：

| 组件 | 是什么 | 详情 |
|---|---|---|
| **[archive-mcp](archive-mcp/)** | 记忆服务：事件为节点，时间/语义/因果三类边织成网络；Louvain 故事群、PPR 联想、因果反思循环、人格档案卡；MCP/REST/viz 三通道 | [archive-mcp/README.md](archive-mcp/README.md) |
| **[agent-cli](agent-cli/)** | 终端客户端：用档案卡拼接唤醒 ta；夜间定时唤醒，ta 自主反思整理记忆 | [agent-cli/README.md](agent-cli/README.md) |

快速开始：先把 [archive-mcp 跑起来](archive-mcp/README.md#快速开始)，再用 [agent-cli 唤醒 ta](agent-cli/README.md#快速开始)。

## 许可证

[AGPL-3.0](LICENSE)——你可以自由使用、修改、部署本项目（包括商用），但基于它的修改在以网络服务形式提供时，必须以同许可证开源你的修改。
