# dsh-dag-orchestrator — 实施任务分解

> 配套 `docs/DESIGN.md` 阅读（D 表示其章节，R 表示其 §10 红线）。任务可独立派发给实现代理；每个任务自包含：目标 / 范围与涉及文件 / 验收标准 / 依赖。
>
> 通用约束（每个任务默认遵守）：
> - **持久化唯一出口**：一切 sqlite 访问仅在 `lib/dag-store.js`（R2/R7，lint 静态强制 T24）；其余模块经 store 的方法/tx()。
> - 测试一律 `node:test`；**零网络、零真实 CLI、零真实模型调用**（fake executor / fake store / fake ctx；R8）。
> - 迁移代码（critical-path / bounded-queue / ready-evaluator / verify-gate / terminal-commit / readiness 段）保持行级等价，仅做 DESIGN 明确要求的改名/收窄/依赖面替换；原注释语义保留。
> - Plain JS ESM；零新增运行时依赖（zod 除外，同家族版本）；`engines >= 22.13`（node:sqlite；不用 `Promise.withResolvers`）。

## 里程碑总览与依赖图

```
M1 最小闭环（agent-only DAG 全链）
  T01 脚手架 → T02 直搬三件 → T03 DagStore(sqlite+链) → T04 spec-validate
  → T05 ready-evaluator 收窄 → T06 admission+executor(native) → T07 engine 单轮
  → T08 工具三件(plan/status/tick) → T09 apply 总装+reconcile(claimed)
M2 重试与恢复
  T10 terminal-commit 重试路径 → T11 dag_control → T12 approval+dag_approve
  → T13 崩溃对账全量(orphaned)+链拒载 → T14 autoTick(可选 Timer)
M3 深组合
  T15 worktrees 引擎缝 → T16 worktree 声明任务 → T17 merge executor
  → T18 verify 契约门 → T19 README/CI/lint 收尾(T24 并行)
```

---

## M1 最小闭环

### T01 仓库脚手架与包清单
- **目标**：新仓库可 `npm install && npm test`（空测试）通过；包身份定稿。
- **范围/文件**：`package.json`（name `dsh-dag-orchestrator`；`type: module`；`engines: {node: ">=22.13"}`；`dsh.bundle.patch: "./cordis.patch.yml"`；deps `zod@^3.23.0`；peerDependencies `@deepseek-ai/{cordis@^4.0.1, dsh-tools@^0.1.0-rc.6, dsh-subagent@^0.1.0-rc.6}`；scripts test/lint/setup:peer）、`.gitignore`、`LICENSE`（MIT）、`test/.smoke.test.js`、`scripts/link-harness-dsh-tools.sh`（动态解析 live 根，禁硬编码——subagents 模式）。
- **验收**：`npm test` 绿；`dsh.bundle.patch` 字段存在；空 `cordis.patch.yml` 可解析。
- **依赖**：无。

### T02 直搬三件（critical-path / bounded-queue / types）+ reflect 工具
- **目标**：纯函数地基就位 —— DESIGN §9.1。
- **范围/文件**：`lib/critical-path.js`（33 行直搬：类型抹除 + spec 形状适配）、`lib/bounded-queue.js`（150 行直搬：BoundedQueue + compareQueueEntries 5 级排序原样）、`lib/types.js`（BlockedReason/ReadyEvaluation/TaskGraphSnapshot/TaskGraphTaskView）、`lib/reflect.js`（~25 行：手写 promise 状态包装器 `reflect(p) → {status:'pending'|'fulfilled', value?, reason?}`，不用 withResolvers）、`test/critical-path.test.js`、`test/bounded-queue.test.js`（comparator 全表：humanUnblocked/criticalPath/priority/readyAt/taskId 五级、capacity 满不抛、drain 排空、remove）。
- **验收**：纯函数表驱动全绿；comparator 与源行为逐例一致（迁移用例不削弱）。
- **依赖**：T01。

### T03 DagStore（node:sqlite + 事件 hash 链）
- **目标**：唯一持久化出口 —— DESIGN §6。
- **范围/文件**：`lib/dag-store.js`、`test/dag-store.test.js`。
  - `createDagStore({path})` → 动态 `import('node:sqlite')`（失败 loud）；`DatabaseSync`；`PRAGMA journal_mode=WAL; busy_timeout=5000; application_id=<魔数>; user_version=1`；库文件 0600 `wx` 创建（已存在则校验 application_id，失配拒开）。
  - DDL：runs/tasks/attempts/events/approvals/outputs 七表（§6.2 原样）。
  - `tx(fn)`：BEGIN IMMEDIATE … COMMIT/ROLLBACK（同步事务）。
  - 投影方法（全参数化预编译 + CAS 形状）：runs：insertRun/findRun/findNonTerminalRuns/casRunState(runId, fromState, version, to, patch)/setControlIntent；tasks：insertTasks/findTasks/casTaskState(runId, taskId, from, version, to, patch?)（patch 含 blockedReason/retryNotBefore）；attempts：insertAttempt/findAttempts(runId, taskId)/findNonTerminalAttempts(runId)/commitTerminal(attemptId, ownerId, target)（**校验 ownerId + state CAS**，对照 taskAttempts.commitTerminal 契约）/hasNonTerminalAttempt；events：insertEvent(runId, {type, taskId?, attemptId?, payload})（**tx 内计算 seq=lastSeq+1、prevHash=last.hash、hash=sha256(prevHash ∥ canonical)**）/findEvents(runId, {afterSeq, limit})/verifyChain(runId)（全量重算，返回 {ok, firstBadSeq}）；approvals：insert/findPending/decide(approvalId, decision, note)；outputs：upsert/findOutput(runId, taskId, name)。
  - canonical JSON：键排序 + 无空格（稳定 hash 的前提，测试断言）。
- **验收**：round-trip 一致；CAS 失配返回 not-ok 不抛；**同事务性测试**——tx 内抛异常 → 投影与事件**双双**回滚（这是不变量 #3/#6 的机械测试）；链校验对篡改 payload/删除事件/乱序均失配；0600 断言；并发 tx 串行（嵌套 tx 拒绝 loud）。
- **依赖**：T01。

### T04 spec-validate（WorkflowSpec 子集，zod strict + 结构规则）
- **目标**：`lib/spec-validate.js` —— DESIGN §7.1/§7.2。
- **范围/文件**：`lib/spec-validate.js`、`test/spec-validate.test.js`。
  - zod strict 全表（§7.1 裁剪表逐字段）：根（version=1/name 1..80/description≤2000/project{root?,baseRef?}/limits{maxRunningAgents 1..32, queueCapacity 1..1024}/tasks 1..256）；task（id 正则/kind enum agent|approval|merge/dependsOn[{taskId, condition enum succeeded|completed}]/inputs task:// URI/outputs[{name, schema(ObjectJsonSchema 子集校验), required?}]/backend enum native|spawn|fork + 委派平铺字段（model/provider/persona/toolFilter/cwd/maxTokens/maxDepth/delegation）+ retry 五字段 + timeoutMs/priority/failurePolicy/concurrencyKey/verify{expectOutput, expectStatus}/approval{action, prompt?}/worktree{task, baseRef?}（M3 字段 M1 即校验、M1 运行时未支持时 dispatch loud））。
  - 结构规则（zod superRefine 或后置）：id 唯一；dependsOn 引用存在 + 无自指 + **无环**（DFS，用 T02 downstream 构造）；inputs 的 producer 是直接/传递上游且声明同名 output；kind 矩阵（approval 禁 retry.maxAttempts>1、禁 delegation 字段；agent 必须 prompt；**`permission_mode`/`reasoning_effort`/未知 backend 出现即 `dag.bridge_unsupported`**（O2 前的结构闸））；每 task 至多 1 output；错误 {code, path, message} 形态（照 workflow-spec.md L326-341）。
  - `specHash(spec)`：默认值填充 + 键排序 → sha256（规范化规则照 workflow-spec.md L313-324 收窄版）。
- **验收**：DESIGN §7.2 示例 spec 通过；未知键/环/悬空依赖/自指/非上游 input/approval retry>1/bridge 字段全部拒且 path 准确；specHash 稳定（同 spec 异键序同 hash）。
- **依赖**：T02（无环检测用 downstream）。

### T05 ready-evaluator 收窄直搬
- **目标**：`lib/ready-evaluator.js` —— DESIGN §9.2 行 4。
- **范围/文件**：`lib/ready-evaluator.js`、`test/ready-evaluator.test.js`。
  - 直搬：`evaluateReady`（444 行收窄到 ~280：裁 sandboxResolver/profileResolver/gateEvaluator 形参——保留调用位注释「M3 gate 回归」）、`upstreamSatisfies`（in-progress/completed 集合 + 三态 verdict 原样）、`isReadySource`（pending 恒真；retry_wait 到期；blocked 重评但 `approval_pending` park 排除）、`READY_BLOCKED_CODES`。
  - 适配：`inputs` 解析器换 outputs 表查询回调（`outputResolver(ref) → boolean`，task:// 语法）；spec 形状 = T04 产物。
- **验收**：迁移语义用例——succeeded 条件 vs completed 条件 vs blocked 上游（**blocked 不算 completed**）；retry_wait 未到期跳过；approval_pending park 不重评；inputs 缺失 → blocked(artifact_missing→dag.output_missing)；fail-closed（resolver 缺席且声明 inputs → blocked）。
- **依赖**：T02 T03（outputResolver 接口形状）T04（spec 形状）。

### T06 Admission（内存信号量）+ Executor（native 绑定）
- **目标**：并发治理 + subagent 换绑核心 —— DESIGN §4.2/§5.4。
- **范围/文件**：`lib/admission.js`（~60：tryAcquireSlot/releaseSlot（maxRunning 上限）、sessionKeys Map 互斥、`tryAcquireSessionKey(key, attemptId)`）、`lib/executor.js`（~180）、`test/admission.test.js`、`test/executor.test.js`（fake ctx.subagents）。
  - `createExecutor({ ctxSubagents, store, execAgent, config })` → `dispatch(task, attempt): {attemptId, reflected}`：
    - prompt 组装（§7.4：内联头 + 分隔符 + 32KiB 上限 → `dag.input_too_large`）；
    - `request` 组装（§4.2 全字段；**DEFAULT_TASK_FILTER = deny [dag_五件, subagent, subagent_fork]**；delegation:true 时移除后两项；cwd 补丁未就位且 task.cwd 存在 → loud `dag.cwd_patch_required` 指引跑 subagents patches/install）；
    - `assertSubagentMaxDepth`（白名单唯一 import，lint 强制）前置断言；
    - `AbortController` + timeout timer（timeoutMs → abort）；`ctxSubagents.start(task.backend ?? 'spawn', request)`；
    - 失败分类（reject → transient `dag.dispatch_failed`）；成功 → inFlight Map 记 `{reflected: reflect(run.result), controller, timer, childSession: run.id}`；
    - `harvest(attemptId)`（§4.5 映射表全实现：completed+output 门 / error→transient / aborted→timeout 或 cancelled / max-tokens / refusal / infraReject）；dispose(attemptId)。
  - fake `ctx.subagents`：`start()` 返回 controllable promise 的 fake run（测试可 resolve 任意 SubagentResult / reject / 挂起）；断言 request 形状（label/prompt/agentOptions/toolFilter deny 集合/outputSchema/cwd）。
- **验收**：映射表逐行用例（6 种 stopReason × output 声明与否）；超时路径（timer→abort→result aborted→failure timeout）；filter deny 集合正确且 delegation 开关生效；dispatch reject → transient；深度断言触发；**不 await result**（dispatch 返回时 promise 悬置——测试断言 fake 未被 await 消费）。
- **依赖**：T03（outputs 读写）。

### T07 DagEngine（单轮 reconcile + tick 多轮）
- **目标**：状态机心脏 —— DESIGN §5。
- **范围/文件**：`lib/engine.js`（~850 含 driver 折叠）、`lib/terminal-commit.js`（~140）、`test/engine.test.js`、`test/terminal-commit.test.js`。
  - `createEngine({ store, executor, admission, config })` → `{ tick(runId, {maxRounds, settleMs}), oneRound(runId), reconcileApprovals, planRun(spec, baseCwd, exec), status(...), control(...) }`。
  - `oneRound` 十步（§5.1 表逐条）：reconcileApprovals（M1 空实现占位）→ promoteReady（evaluateReady → 单 tx：CAS + task.ready/task.blocked 事件 + dependencyGateNotMet 事件位）→ admission 门（controlIntent pause/stop 跳派发）→ buildQueue（T02 直搬件）→ dispatchLoop（每 entry：acquireSlot 失败留 ready；**claimTask 单 tx**：重读行 + hasNonTerminalAttempt + ordinal=prior+1 + ready→queued→running 双 CAS(version 两次递增) + attempt 行 + attempt.claimed/task.queued/task.running 三事件（事件序列照源逐字；投影可在同 tx 内一次走完两跳——DESIGN §6.2 迁移收窄说明）——对照 scheduler-loop L1943-2056 的 H1/TOCTOU 修复；事务外 executor.dispatch；dispatch reject → commitTerminal transient）→ harvestSettled（reflected fulfilled 逐个 commitTerminal——**每 attempt 独立 tx，绝不批量**，不变量 #1）→ propagateDownstream（readiness.ts L165-203 直搬：failed + block_downstream → 下游 blocked(upstream_failed)）→ finalizeRunIfDone（scheduler-driver L401-489 直搬：allTerminal/onlyDeadBlocked 判定 + 聚合 anyFailed/allCancelled/succeeded + FINALIZE_BLOCKING_INTENTS(pause) + 同 tx run.<target> 事件）→ drainToPaused（L758-810 直搬）。
  - `shouldRetry`（L2295-2329 直搬：priorExecutionRetries 计数（events 表查 attempt.retry_scheduled）/maxAttempts 含首次/retryOn 过滤（failureTypeToPolicyKey 映射：timeout→transient_network）/指数退避 + jitter）。
  - `commitTerminalAndRelease`（terminal-commit.ts L80-261 语义直搬）：成功/失败两分支；retry 分支**先 commitTerminal(failed) 再 CAS task→retry_wait**（Issue 5 顺序，L106-125 论证照搬注释）；`attempt.retry_scheduled` 事件；槽/会话键释放在 tx 外。
  - `tick`（§5.2）：maxRounds ≤16 循环 + 零进展时对 run 的 in-flight `boundedRace`——**等待预算全调用累计 ≤ settleMs（60s 上限）、非每轮重置**，`noSettleStreak >= 2`（连续两轮零 settle）即退（防 16×60s 自旋，§5.2 硬保证段）；`boundedRace` 返回 `{anySettled, actualMs}` 预算按实扣；返回 tickSummary（含 waiting_on 判定：nothing/in_flight_attempts/approval/external + next_hint）。测试断言：预置挂起 attempt + maxRounds=16 + settleMs=60_000 → 调用总阻塞 < 单轮耗时 + 61s（预算不被轮数放大）。
- **验收**：端到端 fake 链——3 节点菱形 DAG（A→B,C→D）：首轮 promote A + dispatch + queue B/C；A settle → 次 round 传播 + B/C 派发（并发=2 槽）；全绿 → finalize succeeded + run.succeeded 事件 + 链完好；重试矩阵（transient 且 retryOn 匹配 → retry_wait + backoff；permanent → failed + 下游 blocked）；失败传播（B failed → D blocked(upstream_failed)，C 不受累）；pause 语义（关派发、in-flight 继续、排空后 paused）；**不变量 #1 测试**：多 attempt 同轮 settle → 逐个独立 tx（断言 events 顺序无批量窗口）；链校验在每次操作后仍 ok。
- **依赖**：T03 T05 T06。

### T08 工具三件（dag_plan / dag_status / dag_tick）
- **目标**：模型面 —— DESIGN §8.1-§8.3。
- **范围/文件**：`lib/tools/dag-plan.js` / `dag-status.js` / `dag-tick.js`、`test/tools.test.js`（fake ctx：tools.register 收集器 + fake engine）。
  - schema 逐字对齐 DESIGN §8；返回条件展开（无 undefined 值键）；错误 throw `Error('dag_plan: <code> — <detail>')`；`dag_tick`/`dag_plan` 的 `isConcurrencySafe: () => false`，`dag_status` true。
  - dag_plan：resolveBaseCwd（exec.agent.session.header.cwd；repo-gate 形态门禁——workspaceRegistry 可选探测 + baseCwd 子树 + allowedRoots，缺席降级不炸）→ validate → store tx 插 run（parent_session=exec.agent.session.id）+ tasks + run.created 事件 → 内联首轮 tick → 返回（含 warnings）。
  - description 尾部嵌编排示例（§8.6 ①）。
- **验收**：fake ctx 注册断言（name/description/schema）；plan→tick→status 全链返回形状与 DESIGN 逐字段一致；严格 unknown 参数拒（defineTool 参数面天然）；json-safe（无 undefined 键——对拍 JSON.stringify 无丢失）。
- **依赖**：T07。

### T09 apply() 总装 + reconcile（claimed 分支）+ bundle patch
- **目标**：M1 可安装闭环 —— DESIGN §3/§12。
- **范围/文件**：`lib/index.js`、`lib/config.js`（zod strict：register 开关五件/dbPath 默认 `~/.dsh/dag-orchestrator/dag.db`/defaultMaxRunningAgents=4/defaultQueueCapacity=16/inputInlineLimitBytes=32768/autoTickMs=0/allowedRoots/requireWorkspaceRegistration；未知键 fail loud）、`lib/recovery.js`（M1 版：claimed→failed+retry_wait 分支 + 链校验拒载分支）、`cordis.patch.yml`（单 insert 行，对齐 worktrees §8.3 形态）。
  - `apply(ctx, config)`：validateConfig → `assertSingleDshToolsInstance`（Symbol 自检照 subagents lib/index.js）→ store.open → **reconcile（先于工具注册**，C4/R4）→ executor/engine 装配（execAgentProvider = 每次 exec 传入）→ 注册工具（register 开关）→ `ctx.effect` teardown（dispose 全 in-flight、clear timers、close db）→ **返回 undefined**。
  - `@deepseek-ai/dsh-subagent` import 白名单 lint 位（仅 assertSubagentMaxDepth——subagents 红线 12 同款）。
- **验收**：`test/index.test.js`：装配后 3 工具在场；预置含 claimed attempt 的库 → apply 后该 attempt=failed + task=retry_wait + 事件链完好；config strict 拒未知键；apply 返回 undefined；teardown 幂等。
- **依赖**：T08。

---

## M2 重试与恢复

### T10 终态提交重试路径加固（如 T07 已含则本任务转为专项测试加固）
- **目标**：retry 分支的全崩溃窗口覆盖 —— DESIGN §5.3/§9.2。
- **范围/文件**：`test/terminal-commit.test.js` 扩展。
  - 用例：commitTerminal 失败（ownerId 不符/已终态）→ **不**发 retry_scheduled、task 不动（Issue 5 逆向）；retry 耗尽（prior retries ≥ max-1）→ terminal failed + 下游传播；backoff 计算确定性（注入 fake clock + fake random：base*2^n 截 max + jitter 公式照源）；retryOn 过滤（permanent 不进 retry）。
- **验收**：全部窗口断言含事件序列（attempt.failed → attempt.retry_scheduled → task.retry_wait 同 tx 原子——崩溃注入测试：tx 中途抛 → 三者全无）。
- **依赖**：T07。

### T11 dag_control（pause/resume/stop/retry_task/cancel_task）
- **目标**：控制面 —— DESIGN §8.4。
- **范围/文件**：`lib/tools/dag-control.js`、engine.control 实现、tools.test 扩展。
  - 语义逐条：pause → setControlIntent(pause) + run.control 事件（不碰任务）；resume → paused→running CAS + 清 intent；stop → intent=stop + **dispose 全部 in-flight**（executor.abort(attemptId) 逐个 → result aborted → harvest 走 cancelled 语义 → run cancelling→cancelled）；retry_task → task ∈ {failed 终态, blocked(upstream_failed)} → CAS retry_wait(now) + `task.retry_requested` 事件（**人工重试不计入 retryOn 预算**——与 attempt.retry_scheduled 事件类型区分，对照 H-WP13 override 语义）；cancel_task → pending/blocked → cancelled（事件）。
  - 非法状态 loud（如对 succeeded retry_task → `invalid_task_state`）。
- **验收**：五 action 状态转移矩阵 + 幂等 + 事件成对；stop 的 in-flight 全取消且链完好；retry_task 后 tick 重派发。
- **依赖**：T07（engine.control 可与 T09 并行开发）。

### T12 approval kind + dag_approve + reconcileApprovals
- **目标**：审批内建 —— DESIGN §8.5/§5.1 步 2。
- **范围/文件**：`lib/executors/approval.js`（~90）、`lib/tools/dag-approve.js`、engine.reconcileApprovals、`test/approval.test.js`。
  - executor 侧（照 task-executors.ts L191-256 三分支直搬语义）：已 approved → 成功；已 rejected → permanent `dag.approval_rejected`；否则 approvals.insert(pending, action, prompt) → **park**：commitTerminal(attempt, failed, reason approval_pending) + task CAS blocked({code:'approval_pending', approvalId}) + 事件，同 tx（对照 L239-254）；槽外释。
  - reconcileApprovals（每 tick 首）：blocked(approval_pending) + decision=approved → tx{task blocked→succeeded + task.succeeded 事件(带 approvalId)}；rejected → failed(`policy_denied`)。
  - dag_approve 工具：校验 pending 存在 → tx{approvals.decide + approval.decided 事件}（**不改 task**）→ 返回当前 task_state（blocked，提示下轮 tick 提升）+ approval_prompt 回显；重复决策 → `already_decided`。
  - `isReadySource` 对 approval_pending park 的排除（T05 已埋）在此端到端验证。
- **验收**：序列 plan(含 gate) → tick（gate blocked(approval_pending), waiting_on: approval）→ approve → tick（gate succeeded + 下游派发）；reject 路径 `policy_denied` + 传播；幂等矩阵；事件原子性（崩溃注入）。
- **依赖**：T07 T11。

### T13 崩溃对账全量（orphaned 路径 + 链拒载）+ 恢复语义测试
- **目标**：断点续跑完整 —— DESIGN §12。
- **范围/文件**：`lib/recovery.js` 扩全（§12.2 序列：verifyChain 失配 → run failed + recovery.chain_broken；claimed → failed+retry_wait 自动；running → orphaned + task failed + recovery.action_requested（含 child_session）；outputs 孤儿行审计 warn）、`test/recovery.test.js`。
  - 决策表测试（对照 recovery-service 五分类的 DSH 映射表 §12.1 逐行）：claimed/recovered-auto-retry；running/orphaned/human；链断拒载单 run 不累及其余；pausing 保持。
  - **有界政策断言**：不存在任何「running → 自动成功」路径；唯一自动重试 = never-dispatched。
- **验收**：预置库各窗口 → apply → 断言投影 + 事件 + action_requested 清单；dag_status(events) 可见恢复痕迹；断点续跑端到端（plan → 部分 running → 杀进程（测试模拟：直接改库模拟崩溃前态）→ 重 apply → tick → 终态）。
- **依赖**：T09。

### T14 autoTick 可选 Timer（免派发 reconcile）
- **目标**：无人泵收割 —— DESIGN §5.5。
- **范围/文件**：engine.autoTick（config.autoTickMs 默认 0；>0 时 `ctx.effect(() => setInterval)`）；**只做** reconcileApprovals/harvestSettled/propagate/finalize/drainToPaused——**不派发**（无 exec.agent；注释写明这是随宿主生死模型的诚实边界）；teardown 清理；Timer 回调异常 catch + logger.warn（不炸宿主）。
- **验收**：autoTickMs=50 + fake settle → 无人调 tick 也可收割落账；派发不被 Timer 触发（断言 store 无新 attempt）；0 = 无 Timer（unref 断言可选）。
- **依赖**：T09。

---

## M3 深组合

### T15 worktrees 引擎缝（ctx.get 可选探测 + 缺席降级）
- **目标**：组合通道 —— DESIGN §11.2。
- **范围/文件**：`lib/index.js` **机会主义探测** `ctx.get('worktreesEngine')`（apply 时 + 每次使用时各探一次；**不用 inject**——它会把插件加载阻塞到依赖可用，见 DESIGN §11.2；缺席 → null 不炸）；executor/executors 构造注入 `{worktrees?: {getMergeQueue(), getWorktreeService()}}`；merge/worktree 任务遇缺席 → loud `dag.worktrees_unavailable`（对照 `scheduler.merge_queue_not_configured` L76-83 文案形态）。
- **验收**：fake worktreesEngine 注入 → 引用透传；缺席 → agent-only DAG 全绿且 merge spec 派发时 loud。
- **依赖**：T09。（注：若 dsh-worktrees 尚未实装服务暴露，本任务先以本插件侧接口 + fake 落地，其侧暴露列为其后续任务——接口契约以本任务 JSDoc 为准。）

### T16 worktree 声明任务（dispatch 前创建 + 复用）
- **目标**：写任务隔离 —— DESIGN §11.3。
- **范围/文件**：executor.dispatch 前置分支：task.worktree 存在 → `getWorktreeService().create({task: worktree.task, repoRoot: project.root, baseRef, origin:'dag', correlationId: attemptId})` → path 作 cwd；attempt 终态**不删**（worktrees 生命周期）；retry_task 重派发时检测 active worktree 记录（同 correlation 前缀/task slug）→ 复用 path；创建失败 → transient `dag.worktree_create_failed`。
- **验收**：fake worktree service 断言 create 入参（origin:'dag'/correlationId=attemptId）；cwd 透传进 subagent request；终态后无 cleanup 调用；重试复用。
- **依赖**：T15。

### T17 merge executor（DrainOutcome 五态映射）
- **目标**：串行集成节点 —— DESIGN §11.1。
- **范围/文件**：`lib/executors/merge.js`（~110）、`test/merge-executor.test.js`（fake merge queue）。
  - 输入映射：dependsOn(succeeded) 中带 worktree 声明的上游任务的关联 worktree = 源（engine 从事件/outputs 取 worktreeId 关联）。
  - 执行：每源 `enqueue({worktreeId, integrationBranch, origin:'dag', correlationId})` → `drain(repoKey, branch)` → 五态映射（§11.1 表：succeeded→succeeded(outputs 记 integratedCommit)；conflicted→blocked(merge_conflicted, conflictFiles+retained 路径)——**不终态 failed**，人工 worktree_queue resolve/retry 后 dag_control.retry_task；failed→按 retry 策略；no_changes→succeeded；queued(queued_ahead)→本轮挂起下轮 tick 再 drain）。
- **验收**：五态各一用例 + 事件序列；conflicted → blocked 后 tick 不死循环（isReadySource 对 merge_conflicted 的处理——**新增 park 码**：merge_conflicted 同 approval_pending 排除自评，retry_task 是唯一出口）；与 worktrees 队列语义一致性（fake queue 断言 enqueue/drain 调用形状与其 §10 契约逐字段一致）。
- **依赖**：T15 T16。

### T18 verify 契约门（evaluateVerifyGate 直搬）
- **目标**：验证即契约 —— DESIGN §7.3。
- **范围/文件**：`lib/verify-gate.js`（119 行直搬改造：receipt 源 = outputs 表本 attempt 的 expectOutput 且值.status === expectStatus；无 verify 声明 → evidence 'none_declared'；未过 → 合成 permanent `dag.verify_gate_failed`）、`ready-evaluator.js` 补 `evaluateGate` 五算子直搬（gate 声明随 dependency `gate: {artifact(task://…), expect, value?}`——M3 可选字段）、executor harvest 的 completed 分支接入 gate（fail 后走 commitTerminal(permanent) 而非 succeeded）。
- **验收**：gate 直搬语义表（声明+receipt 过/缺/未过；未声明 none_declared）；evaluateGate 五算子全表（exists/not_exists/contains/not_contains/equals——有限布尔，无脚本面）；gate 声明但无 evaluator fail-closed（dependencyGateEvaluatorUnavailable 码照源）。
- **依赖**：T07（executor harvest 位）T03（outputs 查询）。

### T19 双语 README + CHANGELOG + AGENTS + SECURITY
- **目标**：文档面 —— 家族惯例（subagents「两语言段落对齐」红线）。
- **范围/文件**：`README.md`/`README.zh.md`（定位/安装（dsh plugin add + setup:peer + Node≥22.13 说明）/§8.6 四组合序列/配置表/多实例风险（O5）/O2 bridge 边界/随宿主生死模型说明（tick=泵、autoTick 免派发边界））；`CHANGELOG.md` 0.1.0；`AGENTS.md`（红线 12 条继承 DESIGN §10）；`SECURITY.md`。
- **验收**：两语言段落对齐；示例与工具真实返回字段一致。
- **依赖**：T09（越晚写越接近实现真相，放 M3 收尾但不阻塞发布 M1——M1 发布时可先出最小 README）。

### T24 lint + CI（与 M3 并行，实为全程纪律）
- **目标**：纪律机械化 —— DESIGN §10 R2/R7。
- **范围/文件**：`scripts/lint.js`（`node --check` 全模块 + **sqlite 出口纪律：`lib/` 内 `node:sqlite`/`DatabaseSync` 仅出现在 dag-store.js** + `db.exec(` 仅允许 PRAGMA/DDL 常量形态 + `@deepseek-ai/dsh-subagent` 导入白名单 {assertSubagentMaxDepth}——正反例内嵌测试）；`.github/workflows/ci.yml`（macOS/Ubuntu × Node 22.13/24——**Windows 暂缓**：node:sqlite 在 Win 的 CI 矩阵先验证再上，标注为已知缺口）。
- **验收**：lint 对越界 sqlite/白名单外导入样例报错；CI 绿（全 fake 测试，裸 runner 可跑）。
- **依赖**：T09（此后每任务合入前跑）。

---

## 任务依赖速查

| 任务 | 依赖 |
|---|---|
| T01 | — |
| T02 T03 | T01 |
| T04 | T02 |
| T05 | T02 T03 T04 |
| T06 | T03 |
| T07 | T03 T05 T06 |
| T08 | T07 |
| T09 | T08 |
| T10 T11 T14 | T07/T09 |
| T12 | T07 T11 |
| T13 | T09 |
| T15 | T09 |
| T16 | T15 |
| T17 | T15 T16 |
| T18 | T07 T03 |
| T19 T24 | T09（T24 随全程） |

## 建议派发批次

1. **批 1**：T01 → T02（直搬地基）/ T03（sqlite 心脏）并行（T03 依赖仅 T01）。
2. **批 2**：T04 / T05 / T06 并行（纯逻辑互不依赖，T06 的 fake 先行）。
3. **批 3**：T07（最大单体，独立派）→ T08 → T09。
4. **批 4**（M2）：T10 / T11 / T13 并行 → T12 → T14。
5. **批 5**（M3）：T15 → T16 → T17；T18 独立；T19/T24 收尾。

## 工程量预算（对照 DESIGN §13 / §9.2 规模核算）

| 任务 | 预估 lib 行 | 预估 test 行 | 工期 |
|---|---|---|---|
| T01 | 40 | 5 | 0.25 天 |
| T02 | 260 | 260 | 0.5 天 |
| T03 | 380 | 380 | 1.5 天（链+事务原子性是核心） |
| T04 | 300 | 320 | 1 天 |
| T05 | 280 | 280 | 0.75 天 |
| T06 | 240 | 340 | 1 天 |
| T07 | 990（engine 850 + terminal 140） | 520 | 2.5 天（最大单体） |
| T08 | 300 | 260 | 1 天 |
| T09 | 220 | 220 | 1 天（reconcile 是隐藏工作量） |
| T10 | — | 180 | 0.5 天 |
| T11 | 140 | 200 | 0.75 天 |
| T12 | 200 | 240 | 1 天 |
| T13 | 100（扩展） | 260 | 1 天 |
| T14 | 60 | 80 | 0.25 天 |
| T15 | 60 | 100 | 0.5 天 |
| T16 | 80 | 120 | 0.5 天 |
| T17 | 110 | 180 | 1 天 |
| T18 | 150 | 200 | 0.75 天 |
| T19 | docs ~700 | — | 0.75 天 |
| T24 | 90 | 60 | 0.5 天 |
| **合计** | **~3,100** | **~4,155** | **~18 人日**（单人）；批 2/4 双代理并行可压缩至 ~13 日历日 |

（M1 = T01-T09 ≈ 9.5 人日；M2 = T10-T14 ≈ 4.5 人日；M3 = T15-T19+T24 ≈ 4 人日。）
