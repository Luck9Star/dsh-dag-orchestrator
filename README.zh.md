# dsh-dag-orchestrator

[English](README.md) | **简体中文**

> 适用于 DeepSeek Harness (dsh) `0.1.0-rc.6` / `0.1.0-rc.7` · Node ≥ 22.13（需要 `node:sqlite`）· MIT

把一个多步骤作业变成一张**能断点续跑的并行任务图**。

你只需用 JSON 把作业描述一遍 —— 一组任务和它们的依赖关系（DAG），交给 `dag_plan`，剩下的事插件来干：没有未完成依赖的任务**并行**派发（一个任务 = 一次子代理委派），完成的输出流给下游任务，失败自动重试或向下传播，人工审批门会停下来等你拍板，全部结束后收尾。进度存在本地 SQLite 数据库里 —— dsh 中途崩溃或重启，DAG 都能从断点继续。

## 为什么需要它

"把整个作业委派给一个子代理"有两个问题：

1. **串行执行。** 一个子代理做第 1 步、再第 2 步、再第 3 步 —— 哪怕这些步骤互相独立、本可以三路并行。
2. **重启全丢。** 作业跑到一半宿主重启，进度就没了：哪些做完了没有记录，只能从头再来。

本插件两个问题一起解决：并行（上限 `maxRunningAgents`）+ 持久化（每个状态变更都连同哈希链事件日志一起写入 SQLite；下次启动时对崩溃的 run 做对账，可继续跑）。

## 五个工具

| 工具 | 作用 |
| --- | --- |
| `dag_plan` | 提交任务图（JSON spec）。返回 `run_id` 并完成第一轮派发。 |
| `dag_tick` | 给 run 打气：派发就绪的、收割完成的、传播失败的。反复调用直到终态。 |
| `dag_status` | 查看 run：总览 / 按任务 / 按尝试 / 完整事件日志。 |
| `dag_control` | 暂停 / 恢复 / 停止 run；手动重试或取消某个任务。 |
| `dag_approve` | 回应审批门（`approve` / `reject`）；下一次 tick 按决定继续。 |

## 安装

```sh
# 1. 拉取仓库，并把它链接到正在运行的 dsh 的内部包上
git clone https://github.com/Luck9Star/dsh-dag-orchestrator
cd dsh-dag-orchestrator
npm install
npm run setup:peer        # 避免出现第二份 dsh-tools（没有这一步工具调用会崩）

# 2. 安装进 dsh profile 并重启
dsh plugin --profile web add "$(pwd)"
dsh --profile web         # 开一个新会话
```

**预期结果：** 新会话里出现 `dag_plan`、`dag_status`、`dag_tick`、`dag_control`、`dag_approve` 五个工具。第一次 `dag_plan` 之后，数据库出现在 `~/.dsh/dag-orchestrator/dag.db`。

## 快速上手

一个极简 spec —— 先分析，再两路实现并行跑：

```jsonc
{
  "version": 1,
  "name": "refactor-auth",
  "limits": { "maxRunningAgents": 3, "queueCapacity": 16 },
  "tasks": [
    { "id": "analyze", "kind": "agent", "prompt": "阅读 auth 模块并给出摘要。",
      "outputs": [{ "name": "analysis",
        "schema": { "type": "object", "additionalProperties": false,
                    "properties": { "summary": { "type": "string" } },
                    "required": ["summary"] } }] },
    { "id": "impl-core", "kind": "agent",
      "dependsOn": [{ "taskId": "analyze", "condition": "succeeded" }],
      "inputs": ["task://analyze/analysis"],          // 上游输出内联进提示词
      "prompt": "实现核心改动。上游分析：${inputs}" },
    { "id": "impl-docs", "kind": "agent",
      "dependsOn": [{ "taskId": "analyze", "condition": "succeeded" }],
      "prompt": "更新文档。" }
  ]
}
```

然后驱动它：

```jsonc
dag_plan({ spec })            // → { run_id, task_count: 3, initial_tick: { dispatched: 1, … } }
dag_tick({ run_id })          // → { waiting_on: "in_flight_attempts", … } —— 继续调
dag_tick({ run_id })          // → { run_state: "succeeded", waiting_on: "nothing" }
dag_status({ run_id, detail: "tasks" })   // 终检：每个任务的结果与输出
```

`dag_tick` 会告诉你它在等什么：`in_flight_attempts`（继续 tick）、`approval`（审批门等人）、`external`，或 `nothing`（run 已终态）。

**审批门：** 给任务标 `"kind": "approval"` 并配 `approval` 块。run 到达时，`dag_tick` 返回 `waiting_on: "approval"` 并附审批提示；转达给人类，然后 `dag_approve({ run_id, task_id, decision: "approve" | "reject" })`，再 tick 即可。

## 崩溃恢复，具体是什么体验

- 宿主跑到一半死了 → 下次 dsh 启动时，在任何工具注册之前，插件会逐 run 重新校验事件哈希链并对账：从未派发过的认领自动判失败并重试；已派发但无结果的尝试会挂起，并记一条 `recovery.action_requested` 事件指向对应子会话。
- 你接着调 `dag_status(detail: "attempts")`，视情况 `dag_control(action: "retry_task")`，再 `dag_tick` —— run 继续。
- 跨会话也行：会话 A 规划、会话 B 驱动，活儿照常推进。没有后台守护进程 —— 插件与 dsh 进程同生共死，`dag_tick` 就是泵。

## 任务 spec —— 重要的字段

完整语法见 [docs/DESIGN.md](docs/DESIGN.md)。spec 是 JSON（不是 YAML），严格校验 —— 未知字段、循环依赖、悬空引用、字段用错位置，都会带着明确的 `dag.*` 错误码失败。

| 字段 | 含义 |
| --- | --- |
| `tasks[].id` / `kind` | 唯一 id；`agent`（子代理任务）、`approval`（人工审批门）、`merge`（集成节点，需要 dsh-worktrees）。 |
| `tasks[].prompt` | 子代理拿到的任务文本。 |
| `tasks[].dependsOn` | `[{ taskId, condition: "succeeded" \| "completed", gate? }]` —— 可选产物门（`exists`、`contains` 等）。 |
| `tasks[].inputs` | `["task://<生产者>/<输出名>"]` —— 上游结构化输出内联进本任务提示词。 |
| `tasks[].outputs` | 最多一个具名输出带 JSON schema；子代理的回复先过 schema 校验，下游才会跑。 |
| `tasks[].model` / `provider` / `persona` / `toolFilter` / `cwd` / `maxTokens` | 按任务设置委派参数，字段名与 `subagent` 工具一致。 |
| `tasks[].retry` | `{ maxAttempts, backoffMs, maxBackoffMs, retryOn: ["transient_network" \| "permanent" \| "internal"] }`。 |
| `tasks[].timeoutMs` / `priority` / `failurePolicy` | 超时（默认 30 分钟）；排队优先级；`block_downstream`（默认）或 `isolate`。 |
| `tasks[].worktree` / `merge` | 给任务开独立 git worktree / merge 节点的目标分支 —— 需要 [dsh-worktrees](https://github.com/Luck9Star/dsh-worktrees)。 |

**默认防注入：** 每个任务子代理派发时都被禁用 `dag_*` 与 `subagent*` 工具 —— 任务 agent 永远没法驱动它自己所在的 DAG。上游输出以数据标记包裹内联，绝不当作指令。

## 配置

全部可选 —— 下表每项都有可用默认值。配置写在 profile 的 `cordis.patch.yml` 中本插件的行上；写错的键会在启动时大声报错。

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `dbPath` | `~/.dsh/dag-orchestrator/dag.db` | SQLite 数据库位置（支持 `:memory:`）。一个数据库只服务一个 dsh 宿主。 |
| `defaultMaxRunningAgents` | `4` | 默认并行上限（spec 可覆盖，最大 32）。 |
| `defaultQueueCapacity` | `16` | 默认等待队列容量。 |
| `autoTickMs` | `0`（关） | 自动 tick 间隔；不开就由你（或模型）手动调 `dag_tick`。 |
| `allowedRoots` | `[]` | 任务允许操作的额外仓库根（worktree 在会话目录之外时需要 —— 见下）。 |
| `requireWorkspaceRegistration` | `false` | 把仓库限制为已注册的工作区。 |
| `inputInlineLimitBytes` | `32768` | 内联上游输出的体积上限。 |
| `register.*` | `true` | 按工具注册开关。 |

## 搭配使用

- [dsh-plugin-subagents](https://github.com/Luck9Star/dsh-plugin-subagents)
  —— **按任务 `cwd`（隔离 worktree）必需**。官方宿主会静默丢弃 `cwd`；装上该插件*并跑它的 `patches/install.sh`*，且每次 dsh 升级后重跑。
- [dsh-worktrees](https://github.com/Luck9Star/dsh-worktrees) —— 提供 `worktree:` 任务隔离与 `merge` 节点。一个坑：它的默认 worktree 根（`~/.dsh/worktrees/`）在你的仓库之外 —— 把该路径加进本插件的 `allowedRoots`，否则 worktree 任务建不出来。

## 边界

- **一个数据库，一个宿主。** 不支持两个 dsh 实例写同一个 `dag.db` —— 一个数据库只走一个宿主。
- **与宿主同生共死。** 没有守护进程、没有调度器进程；dsh 没在跑、或者没人 tick（也没设 `autoTickMs`），就没有任何东西在跑。

## 常见问题

| 症状 | 原因 → 处理 |
| --- | --- |
| 每次工具调用都报 `Cannot read properties of undefined (reading 'prepare')` | 出现了两份物理拷贝的 `dsh-tools`。在本仓库重跑 `npm run setup:peer`。 |
| worktree/merge 任务报 `dag.worktrees_unavailable` | 没装 dsh-worktrees（或其 engine face 未加载）。装上，或去掉这类任务。 |
| worktree 任务反复 `dag.worktree_create_failed` | worktree 根不在 `allowedRoots` 里。把 `~/.dsh/worktrees/`（或你配置的根）加进 `allowedRoots`。 |
| 任务的子代理写错了目录 | 宿主把 `cwd` 丢了 —— 装 dsh-plugin-subagents 并跑它的 `patches/install.sh`。 |
| 崩溃后 run 挂起，出现 `recovery.action_requested` | 预期行为。`dag_status(detail: "attempts")`，决定 `retry_task` 还是放弃，然后 `dag_tick`。 |

## 开发

```sh
npm install && npm run setup:peer   # 链接正在运行的宿主的 peers
npm test                            # node --test，全部用假对象 —— 不碰网络、CLI、真实模型
npm run lint
```

设计记录：[docs/DESIGN.md](docs/DESIGN.md)。

## 参考与致谢

- **task-weaver**（`packages/scheduler/`、`recovery-service`）—— 调度器核心（CAS 认领、按尝试的终态事务、事件哈希链、崩溃对账）是它的收敛移植。
- **DeepSeek Harness** 的 `ctx.subagents` API —— 执行面；一个任务节点就是一次程序化子代理委派。
- **dsh-session-query-sqlite** —— `node:sqlite` 使用纪律（WAL、0600 独占创建、`application_id` 归属守卫）沿用其先例。

## 安全

见 [SECURITY.md](SECURITY.md)。数据库仅所有者可读；事件链让每个状态变更都可防篡改、可审计（`dag_status(detail: "events")`）。

## 许可证

[MIT](LICENSE)
