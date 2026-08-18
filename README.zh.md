# dsh-dag-orchestrator

[English](README.md) | **简体中文**

DSH（DeepSeek Harness）的**断点续跑多任务并行 DAG 编排**插件：提交一份严格校验的静态
DAG spec（`dag_plan`），由插件驱动 —— 提升就绪任务、派发（**一个任务节点 = 一次程序化
subagent 委派**，`ctx.subagents.start`）、收割结果、传播失败、终态聚合 —— 泵是模型可见的
工具（`dag_tick`）。一切持久化在单个 `node:sqlite` 库中，**投影变更与其事件同事务提交**，
并以 sha256 hash 链串联；宿主崩溃后，插件在下次启动时对账，DAG 断点续跑。没有独立进程、
没有常驻调度循环 —— 插件**随宿主生死**：`dag_tick` 就是泵。

- 设计记录：[docs/DESIGN.md](docs/DESIGN.md)
- 任务分解：[docs/TASKS.md](docs/TASKS.md)

## 为什么

把多步任务委派给一个子代理，它只能串行跑，宿主一重启就断了线索。本插件把 task-weaver
的 durable scheduler（CAS claim、每 attempt 独立终态事务、事件 hash 链、ready 评估、
失败传播、重试、崩溃对账）落成一套 DSH 工具族，执行层换绑为原生进程内 subagent：
按 `limits.maxRunningAgents` 并行展开、关键路径优先派发、任务间结构化产出流转
（`task://<producer>/<name>`）、审批门、指数退避重试，以及控制面
（pause / resume / stop / retry_task / cancel_task）—— 全部跨宿主重启可续跑。

**随宿主生死**是诚实的生命周期模型：没有守护进程。状态是持久的（sqlite + hash 链 +
重启对账），但「动起来」只发生在有人泵的时候 —— 会话里的 `dag_tick`，或可选的
`autoTickMs` Timer 做免派发 reconcile（见[生命周期模型](#生命周期模型随宿主生死)）。

## 工具族（5 件）

全部是全局层新名字 —— 不接管官方工具、无需 preset 适配、零宿主补丁。除 `dag_status`
外全部 `isConcurrencySafe: false`（它们变更运行状态）。

| 工具 | 作用 |
|---|---|
| `dag_plan` | 提交并严格校验 WorkflowSpec，持久化新 run（全部任务 pending），内联首轮 tick 让进度立现。返回 `{kind:'plan', run_id, spec_hash, task_count, initial_tick, warnings}`。`resume:true` 复用同名非终态 run，而非大声 `dag.name_exists`。 |
| `dag_status` | 只读投影查询：省略 `run_id` 列全部 run 摘要；或按 `detail: summary \| tasks \| attempts \| events` 查单个 run（事件尾窗 `limit` 默认 50，可按 `task_id` 过滤）。 |
| `dag_tick` | 泵：有界多轮 reconcile（`max_rounds` 默认 4 上限 16，`settle_ms` 默认 10000 上限 60000 —— 全调用总预算，不随轮重置）。返回 tick 摘要 —— `run_state`、逐轮计数（`promoted/dispatched/terminal/propagated`）、`in_flight[]`，以及告诉你下一步的 `waiting_on`。 |
| `dag_control` | run 级：`pause`（停止准入、让 in-flight 排空到 paused）、`resume`、`stop`（取消一切；随后要一次 `dag_tick` 完成收割）。节点级：`retry_task`（failed / `blocked(upstream_failed)` / `blocked(merge_conflicted)` → 立即 `retry_wait`；人工重试**不**计入 spec 的 `retryOn` 预算）、`cancel_task`（pending/blocked/ready/queued）。非法源状态大声拒（`dag.invalid_run_state` / `dag.invalid_task_state`）。 |
| `dag_approve` | 决策一个挂起的审批门（`decision: approve \| reject`）：只写决策 + `approval.decided` 事件 —— 绝不直接改任务；下一轮 `dag_tick` 提升（approve → succeeded + 下游派发，reject → failed `dag.policy_denied`）。回显 `approval_prompt` 与 `next_hint`；重复决策大声 `dag.already_decided`。 |

`dag_tick` 摘要形状（字段与实际返回一致）：

```jsonc
{
  "kind": "tick",
  "run_id": "…",
  "run_state": "running",            // running|pausing|paused|succeeded|failed|cancelled
  "rounds": 3,
  "promoted": 4, "dispatched": 3, "terminal": 1, "propagated": 0,
  "in_flight": [ { "task_id": "impl-core", "attempt": 1, "started_at": 1755…, "elapsed_ms": 412 } ],
  "waiting_on": "in_flight_attempts", // nothing|in_flight_attempts|approval|external
  "next_hint": "call dag_tick again after in-flight work settles"
}
```

`waiting_on` 就是方向盘：`in_flight_attempts` → 等工作落地后再 tick；`approval` →
把该门的 `approval_prompt` 复述给用户，再 `dag_approve`；`external` → merge 节点停在
冲突上（先解 worktrees 队列）或上游阻塞等待；`nothing` + 终态 `run_state` → 完成。

## 组合示例（四序列）

### ① 纯 agent DAG

```jsonc
dag_plan(spec)   // → { kind:'plan', run_id:'…', task_count:5,
                 //     initial_tick:{ kind:'tick', dispatched:1, … } }
dag_tick(run_id) ×N
  // waiting_on:'in_flight_attempts' → … → { run_state:'succeeded',
  //   waiting_on:'nothing', next_hint:'run is terminal (succeeded)' }
dag_status(run_id, detail:'tasks')   // 终局核对：counts + 逐任务状态
```

### ② 崩溃续跑

宿主在 run 中途挂了。下次启动时 `apply()` 在**任何工具注册之前**对账：逐 run 重验事件
链；每个 in-flight attempt 被分类 —— 从未派发的 claim 自动置 failed 并立即自动重试
（`recovery.no_dispatch`），已派发的置 `orphaned`、其任务 `failed`，并落
`recovery.action_requested` 事件（携带 `child_session` 供人工核对）。

```jsonc
dag_status(detail:'attempts')        // 看到 orphaned 痕迹 + child_session
dag_control(run_id, action:'retry_task', task_id:'impl-core', reason:'rechecked')
   // → { kind:'control', action:'retry_task', run_state:'running',
   //     effected:[{task_id:'impl-core', from:'failed', to:'retry_wait'}] }
dag_tick(run_id) ×N                  // 续跑到终态
```

### ③ 审批门

```jsonc
dag_tick(run_id)
  // → { waiting_on:'approval', next_hint:'ask the user, then dag_approve' }
  //    （gate 任务停在 blocked(approval_pending)）
dag_approve(run_id, task_id:'gate', decision:'approve', note:'用户同意')
  // → { kind:'approve', decision:'approve', task_state:'blocked',
  //     approval_prompt:'两个实现分支已就绪，是否继续集成？',
  //     next_hint:'call dag_tick to promote the decided approval' }
dag_tick(run_id)                     // gate succeeded → 下游派发
```

### ④ 与 dsh-worktrees 深组合（M3）

spec 的写任务声明 `worktree: {task, baseRef?}`、集成节点是 `kind: 'merge'`：

```jsonc
dag_plan(spec)      // worktree 写任务 + 一个 merge 节点
dag_tick(run_id) ×N
  // 每个 worktree 任务：派发前引擎请求 worktrees 服务创建（或复用）worktree
  // （origin 'dag'、correlationId = attemptId）—— worktree path 即子代理 cwd；
  // worktree id 盖进 attempt.dispatched 事件，供 merge 侧取源。
  // merge 任务：把每个声明 worktree 的 succeeded 上游入队（origin 'dag'），
  // 再 drain 集成分支 ——
  //   succeeded/no_changes → 任务 succeeded（outputs 记 integratedCommit）；
  //   conflicted → 任务 blocked(merge_conflicted)、waiting_on:'external'，
  //                next_hint:'… worktree_queue resolve/retry, then
  //                           dag_control retry_task'
worktree_queue(action:'resolve', …)          // dsh-worktrees 侧，人工
dag_control(run_id, action:'retry_task', task_id:'integrate')
dag_tick(run_id) ×N
```

**诚实的状态说明**：这组组合要求 dsh-worktrees 把其引擎经 Cordis 服务面暴露出来
（`ctx.get('worktreesEngine')`，含 `getMergeQueue()` / `getWorktreeService()`）。
该 provider 侧暴露**尚未实装**（已核实 —— 其 `lib/` 中无任何 `provide` /
`worktreesEngine`）；消费侧契约以 `lib/worktrees-seam.js` 的 `WorktreesEngineFace`
JSDoc 为准。在它落地之前，含 worktree 或 merge 任务的 spec 会在派发时对这些节点**大声**
报永久 `dag.worktrees_unavailable`；**纯 agent DAG 完全不受影响**。

## 安装

三步（家族惯例）：

```sh
# 1. 安装插件（reconcile 自动追加 bundle 层 —— 单行 insert、id 'dag'；
#    不 disable 任何官方行：dag_* 全是新名字）
dsh plugin --profile web add dsh-dag-orchestrator      # 或：add <本地路径>

# 2.【本地检出必跑】dsh-tools 单实例链接（peer 纪律）
cd dsh-dag-orchestrator && npm install && npm run setup:peer

# 3. 重启 dsh，开新会话
dsh --profile web
```

- **本地开发安装第 2 步必跑。** `@deepseek-ai/{cordis,dsh-tools,dsh-subagent}` 是
  peerDependencies。npm ≥7 会把 peer 装成真实目录（第二份物理拷贝），破坏宿主的单实例
  不变量 —— 第二份 `dsh-tools` 会在自己的模块级 Symbol 下注册第二套 tool-runtime
  调度器，此后每个工具调用都死在
  `Cannot read properties of undefined (reading 'prepare')`。
  `npm run setup:peer`（scripts/link-harness-dsh-tools.sh）把三个 peer 一并 symlink
  到正在运行的宿主所用的那份拷贝。它动态解析 live 根（`command -v dsh` → realpath →
  向上走到 `node_modules`，或 `DSH_HARNESS_ROOT` 显式覆盖 —— 绝不硬编码缓存路径），
  解析不到就大声失败。本仓每次 `npm install` 后、每次 dsh 升级后都要重跑。插件在
  apply 时也会自检，检测到第二实例时直接指出这个修法。
- **环境要求**：Node **≥ 22.13**（`node:sqlite`；`engines` 已锁定 —— 动态
  `import('node:sqlite')` 失败即大声拒启，绝不静默降级）。宿主自身满足此要求。
- **无互斥**：`dag_*` 是新名字 —— 本插件与 `dsh-plugin-subagents`（执行层）、
  `dsh-worktrees`（M3 组合面）并存，且这两位正是推荐的搭子。
- 配置覆盖写在 profile 层同 id 行上，见[配置](#配置)。

## 配置

配置挂在 `cordis.patch.yml` 贡献的 `dag` insert 行上。zod strict 校验 —— 未知或拼错的
键在 apply 时大声失败（`dag.config_invalid`）。

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `register.plan` … `register.approve` | boolean | `true` | 逐工具注册开关（plan / status / tick / control / approve） |
| `dbPath` | string | `~/.dsh/dag-orchestrator/dag.db` | sqlite 库路径（前导 `~` 展开；测试可用 `:memory:`） |
| `defaultMaxRunningAgents` | integer 1..32 | `4` | spec 省略 `limits.maxRunningAgents` 时每 run 的子代理并发上限 |
| `defaultQueueCapacity` | integer 1..1024 | `16` | spec 省略 `limits.queueCapacity` 时的就绪队列容量（溢出者留 ready，下轮重评） |
| `inputInlineLimitBytes` | positive integer | `32768` | prompt 内联单输入上限；超限大声报永久 `dag.input_too_large`（绝不静默截断） |
| `autoTickMs` | integer ≥ 0 | `0` | 免派发 reconcile 的可选 Timer 间隔（默认关；见生命周期模型） |
| `allowedRoots` | string[] | `[]` | 派发时 cwd 门禁的额外放行根（在 run 基准 cwd 子树之外追加） |
| `requireWorkspaceRegistration` | boolean | `false` | `true` 对一切显式 `task.cwd` fail CLOSED：宿主侧尚无 workspace 注册查询通道，一个要求宿主拿不出注册表的开关绝不能退化成空操作 |

## WorkflowSpec

`dag_plan` 的 `spec` 参数是一份 JSON 文档，由 `lib/spec-validate.js` **严格**校验
（每层 zod strictObject + 结构规则）。未知键、环、悬空依赖、不可达 inputs、kind 字段
矩阵违规 —— 一律以稳定的 `dag.*` 错误码与精确路径（`tasks[3].dependsOn[0].taskId`）
大声拒绝，且逐条聚合在一次报错里。绝不猜测、绝不降级执行。

```jsonc
{
  "version": 1,
  "name": "refactor-auth",
  "description": "Parallel refactor with review gate and integration",
  "project": { "root": "/abs/repo", "baseRef": "HEAD" },   // root 缺省 = 会话 cwd
  "limits": { "maxRunningAgents": 3, "queueCapacity": 16 },
  "tasks": [
    { "id": "analyze", "kind": "agent", "prompt": "…",
      "outputs": [{ "name": "analysis",
        "schema": { "type": "object", "additionalProperties": false,
                    "properties": { "summary": { "type": "string" } },
                    "required": ["summary"] } }] },
    { "id": "impl-core", "kind": "agent",
      "dependsOn": [{ "taskId": "analyze", "condition": "succeeded" }],
      "inputs": ["task://analyze/analysis"],
      "model": "kimi-code/k3", "persona": "…", "delegation": false,
      "retry": { "maxAttempts": 3, "backoffMs": 5000, "maxBackoffMs": 60000,
                 "retryOn": ["transient_network"] },
      "timeoutMs": 1800000 },
    { "id": "gate", "kind": "approval",
      "dependsOn": [{ "taskId": "impl-core", "condition": "succeeded" }],
      "approval": { "action": "approve_integration",
                    "prompt": "两个实现分支已就绪，是否继续集成？" } },
    { "id": "verify", "kind": "agent",
      "dependsOn": [{ "taskId": "gate", "condition": "succeeded" }],
      "prompt": "在集成分支跑测试并报告…",
      "outputs": [{ "name": "report",
        "schema": { "type": "object", "additionalProperties": false,
                    "properties": { "status": { "enum": ["passed", "failed"] } },
                    "required": ["status"] } }],
      "verify": { "expectOutput": "report", "expectStatus": "passed" } }
  ]
}
```

形状要点（完整语法见 `lib/spec-validate.js`）：

- **任务 kind**：`agent`（一次 subagent 委派）、`approval`（一个门；禁委派字段、禁
  `retry.maxAttempts > 1`）、`merge`（M3；计划时即要求 ≥1 个声明 `worktree` 的
  succeeded 上游）。`worktree.task` slug 在整个 spec 内唯一
  （`dag.worktree_slug_conflict`）—— slug 命名**一个**任务的 worktree，不是共享原语。
- **逐任务委派字段**（平铺，与 `subagent` 工具参数同名）：`backend`
  （`native|spawn|fork`；`native` 是 `spawn` 的别名 —— harness 只注册这两个
  in-process provider 名，executor 在派发时做别名映射，`native` 任务不会撞
  `NO_PROVIDER`）、`model`、`provider`、`persona`、`toolFilter`、`cwd`、
  `maxTokens`、`maxDepth`、`delegation`。
- **toolFilter 地板（红线 5）**：每个任务子代理都带着 `deny [dag_plan,
  dag_status, dag_tick, dag_control, dag_approve, subagent, subagent_fork]`
  这层结构性地板派发。地板只能收窄：spec `deny` 只追加、`allow` 原样透传但
  deny 地板不动、`delegation: true` 只移除两个 `subagent*` 名。指向本部署**未
  注册**工具的条目（`register` 关掉的 `dag_*`、未装 subagents 插件宿主上的两个
  委派工具）在派发时从 filter 裁掉 —— harness 的 `tools.restrict()` 对未知名
  直接 throw，而 deny 一个不存在的工具本就无意义。已注册的 `dag_*` 工具永不可
  被放开。**spec 中手写的工具名请与本宿主实际注册的工具名核对**——在无法探测
  工具注册表的宿主上，未注册名可能在子代理创建时报错。
- **依赖**：`{taskId, condition: succeeded|completed}` —— 不引入表达式语言。可选
  `gate: {artifact, expect, value?}`（T18）：对上游产出的有限五算子布尔检查
  （`exists | not_exists | contains | not_contains | equals`）—— 不是脚本面。
- **inputs/outputs**：`inputs: ["task://<producer>/<name>"]` 对 outputs 表解析；
  producer 必须是直接/传递上游且声明了同名 output；每 task 至多一个 output（其
  ObjectJsonSchema 原样传作子代理的 `outputSchema`）。
- **verify 即契约门**（无 verify 执行器）：声明了 `verify: {expectOutput,
  expectStatus}` 的任务只有当**本次 attempt** 产出该 output 且 `value.status ===
  expectStatus` 才能到 `succeeded`；不满足则合成永久 `dag.verify_gate_failed`。
  未声明 verify → evidence `none_declared`，门直接跳过。该门同时覆盖 agent 任务
  （harvest 时）与 merge 任务（merge executor 成功路径上）—— merge 的 receipt 即
  integratedCommit output，其 status 视图就是 `integratedCommit` 本身，因此把
  verify 钉在期望 commit 上即可对集成节点收门。
- **重试**：`retry: {maxAttempts, backoffMs, maxBackoffMs, jitterRatio, retryOn}` ——
  指数退避 + 抖动，`retryOn` 按策略键过滤（`transient_network | permanent |
  internal`；超时类失败归入 `transient_network` 重试）。
- **bridge 边界（结构性，O2）**：`permission_mode` / `reasoning_effort` 键与
  `native|spawn|fork` 之外的任何 `backend` 都被 `dag.bridge_unsupported` 拒绝 ——
  本插件只绑定原生进程内 subagent；带 settings 的 bridge 委派是未来的缝（见
  subagents 插件的引擎级 dispatch 缝），绝不做静默忽略的参数。

## 生命周期模型（随宿主生死）

- **`dag_tick` 是泵。** 一次调用 = 有界多轮 reconcile：审批对账 → 提升就绪 → 准入门 →
  建队列 → 派发循环（claim + 启动 subagent，事务之外）→ 收割已 settle 的 attempt
  （每 attempt 一个独立事务 —— 绝不批量）→ 传播失败 → run 终态聚合 → pause 排空。
  某轮零进展且有 attempt 在飞时，调用可以等它们 settle，但受**全调用** `settle_ms`
  总预算约束（不随轮重置；另有连续两轮零 settle 即退与 `max_rounds` 上限）—— 一次
  tick 永远不会自旋。
- **派发只发生在工具 exec 内。** 每次委派都挂在调用该工具的 live Agent（当前泵者）
  上 —— Timer 上下文没有 live agent，而缓存 Agent 句柄跨宿主生命周期违背随宿主生死
  模型。跨会话续跑天然成立：A 会话 plan、B 会话 tick，工作继续。
- **`autoTickMs`（默认 0 = 关）是诚实的 Timer。** 大于 0 时，一个 `setInterval` effect
  对每个非终态 run 跑**免派发 reconcile**：审批提升、收割已 settle 的 attempt、
  ready 提升、失败传播、终态聚合、pause 排空 —— 一切除派发之外的事。被提升为 ready
  的任务就等下一次真实 `dag_tick` 来派发（这是该边界的既定代价）。Timer 回调永不向
  宿主抛异常。
- **崩溃对账先于工具注册。** `apply()` 时逐 run 重验事件链（失配则该 run 置 failed +
  `recovery.chain_broken`；其余 run 不受累），从未派发的 claim 立即自动重试，已派发
  的孤儿 attempt 置 `orphaned` 并落人工动作事件 —— 有界政策：唯一的自动动作是重试
  可证明从未跑过的；其余一律路由给人。
- **teardown** 释放全部 in-flight 子代理、清扫引擎、关库（幂等），且 `apply()` 返回
  `undefined`（Cordis loader 契约）。

## dsh-worktrees 组合（M3）

见上方序列 ④。组合缝是**机会主义**的：插件在 apply 时探一次 `ctx.get
('worktreesEngine')`（为了日志里的诚实可用性陈述），并在每次使用时重探 —— 不用
`inject`（可选缝不能把插件加载阻塞到依赖出现为止）。仅当探测值同时以函数形态暴露
`getMergeQueue()` 与 `getWorktreeService()` 才被接纳；其余一律视同缺席。DAG 绝不
自建第二个 merge queue 指向同一 worktrees 状态（其 §10 明令禁止），也绝不删除
worktree —— worktree 生命周期归 worktrees 插件；DAG attempt 的终态把 worktree 留给
merge 收集或人工清理。worktree 复用范围限定在**同任务**重派发（`retry_task` / 重试
策略，DESIGN §11.3）：`worktree.task` slug 全 spec 唯一（计划时
`dag.worktree_slug_conflict`）；派发时仅当 active 记录的 `correlationId` 属于该任务
自己的 attempt 历史才复用 —— 无法证明归属（或属于他任务）的记录转走 create，后者
对被占 slug 大声报 `dag.worktree_create_failed`，绝不静默共用检出。
在 dsh-worktrees 侧服务暴露落地之前，merge/worktree 任务大声报
`dag.worktrees_unavailable`，纯 agent DAG 不受影响。**真实组合时请对齐两插件根路径**：
dsh-worktrees 的 `worktreeRoot`（默认 `~/.dsh/worktrees/`，位于仓库子树**之外**）必须落在
本插件的 cwd 门禁放行范围内（`config.allowedRoots` 或 run 的基准 cwd 子树）——否则引擎提供的
worktree path 会被本插件 cwd 门禁拒绝，任务在 transient `dag.worktree_create_failed` 上反复重试。
要么把 dsh-worktrees 的 `worktreeRoot` 设到仓库内，要么在本插件 `allowedRoots` 加入该根。

## 多实例边界（一个库，一个宿主）

WAL + `busy_timeout = 5000` 保证别的进程恰好读时不损坏文件，但**跨实例并发写不受
支持**：两个 dsh 宿主同时 tick 同一库，后写者大声 busy 超时失败（可接受：不损坏、
不静默）。每个 DAG 数据库只经唯一一个 DSH 宿主操作。库文件属主私有创建（`0600`、
独占 `wx`、父目录 `0700`），并有 `application_id` 魔数守卫 —— 外来或版本不符的库
文件拒开。

## 开发

```bash
npm install
npm run setup:peer     # 把正在运行的 harness 的 @deepseek-ai peers symlink 进来（见上）
npm test               # node:test —— 508 例；全 fake，零网络/零 CLI/零真实模型
npm run lint           # node --check 全模块 + 纪律审计（见下）
```

测试套件绝不依赖真实 CLI、密钥、网络或 live harness —— 只用 fake
executor/store/ctx 即可在裸 runner 上全绿（peers 按 lockfile 从 npm registry 解析；
CI 正是这么跑的）。CI（`.github/workflows/ci.yml`）跑 macOS/Ubuntu × Node
22.13/24：`npm ci` → `npm run lint` → `npm test`。Windows 刻意暂缓，待 `node:sqlite`
在 Windows CI runner 上验证后再上。

`npm run lint` 机械化执行仓库核心纪律：`node:sqlite` / `DatabaseSync` 只允许出现在
`lib/dag-store.js`（唯一持久化出口）；`db.exec(` 参数白名单为模块常量 PRAGMA/DDL
（用户数据永不进入 SQL 字符串）；对 `@deepseek-ai/dsh-subagent` 的导入是纯函数白名单
（`assertSubagentMaxDepth`，仅 `lib/executor.js`）。

## 安全

任务 prompt 是**数据**、DAG 控制面默认对任务子代理 deny、显式 task cwd 过 fail-closed
realpath 门、bridge 越权字段被结构性拒绝。见 [SECURITY.md](SECURITY.md)。

## License

MIT
