# dsh-dag-orchestrator — 文档导航

> 状态：实现完成并经三轮里程碑评审（2026-08-17）。设计三件套已定稿并按其全量落地
> （T01-T18，M1+M2+M3）；交付文档（双语 README / CHANGELOG / AGENTS / SECURITY）
> 与 CI 已就位。三轮评审（M1 有条件通过→修复；M2 有条件通过→修复；M3 有条件通过
> →修复）的全部 Blocker/Major 均已修复并回归（493/493 测试绿、lint clean），
> 首个可用版本 0.1.0 达成发布条件。

## 这是什么

DSH（DeepSeek Harness）插件：**断点续跑的多任务并行 DAG 编排**。上游能力来自
task-weaver 的 durable scheduler（CAS claim、每 Attempt 独立终态提交、状态变更与
Event 同事务 + hash 链、ready 评估、失败传播、重试、崩溃对账），执行层换绑为
DSH subagent 工具族——一个 DAG 任务节点 = 一次程序化 subagent 委派
（`ctx.subagents.start`），不再 spawn 外部 CLI 进程。

**随宿主生死**：插件自带 node:sqlite 持久化 + 重启对账；DSH 重开后断点续跑；
不做独立进程、不做常驻调度循环——`dag_tick` 工具就是泵。

## 文档地图

| 文档 | 内容 | 读者 |
|---|---|---|
| **[DESIGN.md](./DESIGN.md)** | 架构设计定稿（753 行）：已拍板决策 19 项、目标/非目标、执行层换绑（§4）、tick 模型（§5）、持久化选型（§6）、WorkflowSpec 子集（§7）、工具面 JSON schema 级签名（§8）、迁移对照表（§9）、安全红线 12 条（§10）、worktrees 组合（§11）、崩溃恢复分类学（§12）、开放问题 6 项（§14，已全部拍板） | 实现者必读；评审者通读 |
| **[TASKS.md](./TASKS.md)** | 实施任务分解（254 行）：20 个任务（T01-T19+T24），M1 最小闭环 → M2 重试/恢复 → M3 approval+worktrees 深组合三里程碑；每任务目标/范围/验收/依赖；派发批次与工程量预算 | 实现代理的派发工件（现已全部完成） |
| 本 README | 导航 | 所有人 |

仓库根部另有：`README.md` / `README.zh.md`（双语用户文档：五工具、四组合序列、
配置表、生命周期模型）、`CHANGELOG.md`（0.1.0 全量首版）、`AGENTS.md`（实现者
红线）、`SECURITY.md`（安全边界）、`.github/workflows/ci.yml`（CI）。

## 快速理解（五分钟版）

1. **模型**：`dag_plan(spec)` 提交静态 DAG（JSON、strict 校验）→ `dag_tick` 反复泵
   （一轮 reconcile：ready 提升 → 准入 → 派发 subagent → 收割终态 → 失败传播 →
   Run 终态投影）→ `dag_status` 查投影 → 审批节点用 `dag_approve` 决策 →
   `dag_control` pause/resume/stop/重试。
2. **执行**：任务节点经 `ctx.subagents.start('spawn', {prompt, agentOptions, toolFilter, cwd…})`
   程序化派发 native 子代理；engine 持有 `run.result` promise，tick 收割终态；
   子代理默认 toolFilter deny 掉 `dag_*` 与 `subagent*`（防注入）。
3. **持久化**：node:sqlite 单库（WAL、0600）；投影与事件**同事务** + sha256 hash 链；
   崩溃后 apply() 先对账（claimed→自动重试；running→orphaned+人工路由）再注册工具。
4. **组合**：与 dsh-plugin-subagents（执行层）、dsh-worktrees（M3：worktree 写隔离
   + merge 队列复用）正交组合，零代码耦合。

## 关联仓库

- `~/Documents/dev/Agents/task-weaver` — 移植源（`packages/scheduler/` 15 文件 ~6.1k 行
  + `packages/application/src/services/recovery-service.ts` 1269 行）。
- `~/Documents/dev/Agents/dsh/dsh-worktrees` — 先行插件；其 DESIGN §10 组合缝是本插件
  M3 merge 节点的复用接口（其侧 `worktreesEngine` 服务暴露尚未实装——消费侧契约见
  本仓 `lib/worktrees-seam.js`，缺席时 merge/worktree 任务 loud
  `dag.worktrees_unavailable`，纯 agent DAG 不受影响）。
- `~/Documents/dev/Agents/dsh/dsh-plugin-subagents` — 执行层绑定对象；其
  `docs/task-weaver-integration-analysis.md` 是本设计的分析基线。

## 阶段状态

- [x] 设计定稿（DESIGN.md / TASKS.md / README.md）
- [x] M1 最小闭环（agent-only DAG 全链，T01-T09）
- [x] M2 重试与恢复（T10-T14：terminal-commit 重试 / dag_control / approval /
      崩溃对账全量 / autoTick）
- [x] M3 approval + worktrees 深组合（T15-T18：引擎缝 / worktree 任务 / merge
      executor / verify 契约门）；T19 文档 + T24 lint/CI 收尾
- [x] M3 评审通过（0 Blocker；2 Major 已修复回归：worktree slug 隔离双层门 + merge verify 门）
- [x] 首个可用版本 0.1.0（493/493 测试、lint clean、CI 就绪）
