# dsh-dag-orchestrator

**English** | [简体中文](README.zh.md)

> **DSH compatibility:** `0.1.0-rc.7` (npm latest) and `0.1.0-rc.6`.
> `peerDependencies: ^0.1.0-rc.6` satisfies `rc.7` under semver (the
> same-version-tuple prerelease rule; `0.1.1-rc.x` would not satisfy).
> Verified against rc.7 — `defineTool` / `TOOL_RUNTIME_SCHEDULER` exports
> intact, the new `DefineToolOptions` fields are all optional, and the
> full suite (514/514) is green with the peers linked to rc.7.
> Node ≥ 22.13. MIT.

A DeepSeek Harness (DSH) plugin for **resumable multi-task parallel DAG
orchestration**: submit a strictly-validated static DAG spec
(`dag_plan`), and the plugin drives it — promoting ready tasks, dispatching
one **task node = one programmatic subagent delegation**
(`ctx.subagents.start`), harvesting results, propagating failures,
finalizing the run — pumped by a model-visible tool (`dag_tick`). Everything
is persisted in a single `node:sqlite` database where **projection changes
and their events commit in one transaction**, chained by a sha256 hash
chain; after a host crash, the plugin reconciles on the next start and the
DAG resumes. No separate process, no resident scheduler loop — the plugin
**lives and dies with the host**: `dag_tick` is the pump.

- Design record: [docs/DESIGN.md](docs/DESIGN.md)
- Task breakdown: [docs/TASKS.md](docs/TASKS.md)

## Why

Delegate a multi-step job to one subagent and it runs serially, and a host
restart loses the plot. This plugin turns task-weaver's durable scheduler
(CAS claims, one-transaction-per-attempt terminal commits, event hash
chains, ready evaluation, failure propagation, retries, crash
reconciliation) into a DSH tool family whose execution layer is rebound to
native in-process subagents: parallel fan-out up to `limits.maxRunningAgents`,
critical-path-priority dispatch, structured outputs flowing between tasks
(`task://<producer>/<name>`), approval gates, retry with exponential
backoff, and a control plane (pause / resume / stop / retry_task /
cancel_task) — all resumable across host restarts.

**Dies with the host** is the honest lifecycle model: there is no daemon.
State is durable (sqlite + hash chain + restart reconciliation), but motion
happens only when someone pumps — `dag_tick` from a conversation, or the
optional `autoTickMs` Timer for the no-dispatch reconcile (see
[The lifecycle model](#the-lifecycle-model-dies-with-the-host)).

## The tool family (5 tools)

All new global-layer names — no official tool takeovers, no preset
adaptation, zero host patches. Every tool except `dag_status` is
`isConcurrencySafe: false` (they mutate run state).

| Tool | What it does |
|---|---|
| `dag_plan` | Submit + strictly validate a WorkflowSpec, persist a new run (all tasks pending) with an inline first tick so progress is visible immediately. Returns `{kind:'plan', run_id, spec_hash, task_count, initial_tick, warnings}`. `resume:true` re-attaches to a same-name non-terminal run instead of the loud `dag.name_exists`. |
| `dag_status` | Read-only projection query: all-runs summary (omit `run_id`) or one run at `detail: summary \| tasks \| attempts \| events` (events tail window, `limit` default 50, filterable by `task_id`). |
| `dag_tick` | The pump: a bounded multi-round reconcile (`max_rounds` default 4 max 16, `settle_ms` default 10000 max 60000 — a whole-call budget, never reset per round). Returns the tick summary — `run_state`, per-round counters (`promoted/dispatched/terminal/propagated`), `in_flight[]`, and `waiting_on` which says what to do next. |
| `dag_control` | Run level: `pause` (stop admitting, let in-flight drain to paused), `resume`, `stop` (cancel everything; a follow-up `dag_tick` finishes the harvest). Node level: `retry_task` (failed / `blocked(upstream_failed)` / `blocked(merge_conflicted)` → immediate `retry_wait`; manual retries are NOT billed against the spec `retryOn` budget), `cancel_task` (pending/blocked/ready/queued). Illegal source states fail loud (`dag.invalid_run_state` / `dag.invalid_task_state`). |
| `dag_approve` | Decide a pending approval gate (`decision: approve \| reject`): records the decision + `approval.decided` event only — it never touches the task; the next `dag_tick` promotes (approve → succeeded + downstream dispatch, reject → failed `dag.policy_denied`). Echoes `approval_prompt` and `next_hint`; re-deciding fails loud `dag.already_decided`. |

The `dag_tick` summary shape (fields as actually returned):

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

`waiting_on` is the steering wheel: `in_flight_attempts` → tick again after
work settles; `approval` → relay the gate's `approval_prompt` to the user,
then `dag_approve`; `external` → a merge node parked on a conflict (resolve
the worktrees queue first) or a blocked-upstream wait; `nothing` + a
terminal `run_state` → done.

## Composition walkthrough (4 sequences)

### ① Pure agent DAG

```jsonc
dag_plan(spec)   // → { kind:'plan', run_id:'…', task_count:5,
                 //     initial_tick:{ kind:'tick', dispatched:1, … } }
dag_tick(run_id) ×N
  // waiting_on:'in_flight_attempts' → … → { run_state:'succeeded',
  //   waiting_on:'nothing', next_hint:'run is terminal (succeeded)' }
dag_status(run_id, detail:'tasks')   // final check: counts + per-task states
```

### ② Crash resume

The host died mid-run. On the next start, `apply()` reconciles **before any
tool is registered**: the event chain is re-verified per run; every in-flight
attempt is classified — a never-dispatched claim auto-fails and auto-retries
(`recovery.no_dispatch`), a dispatched one is parked `orphaned` with its task
`failed` and a `recovery.action_requested` event carrying the `child_session`
for manual inspection.

```jsonc
dag_status(detail:'attempts')        // see the orphaned traces + child_session
dag_control(run_id, action:'retry_task', task_id:'impl-core', reason:'rechecked')
   // → { kind:'control', action:'retry_task', run_state:'running',
   //     effected:[{task_id:'impl-core', from:'failed', to:'retry_wait'}] }
dag_tick(run_id) ×N                  // resumed to terminal
```

### ③ Approval gate

```jsonc
dag_tick(run_id)
  // → { waiting_on:'approval', next_hint:'ask the user, then dag_approve' }
  //    (the gate task sits blocked(approval_pending))
dag_approve(run_id, task_id:'gate', decision:'approve', note:'user said go')
  // → { kind:'approve', decision:'approve', task_state:'blocked',
  //     approval_prompt:'两个实现分支已就绪，是否继续集成？',
  //     next_hint:'call dag_tick to promote the decided approval' }
dag_tick(run_id)                     // gate succeeded → downstream dispatches
```

### ④ Deep composition with dsh-worktrees (M3)

A spec whose write tasks declare `worktree: {task, baseRef?}` and whose
integration node is `kind: 'merge'`:

```jsonc
dag_plan(spec)      // worktree write tasks + a merge node
dag_tick(run_id) ×N
  // each worktree task: BEFORE dispatch the engine asks the worktrees
  // service to create (or reuse) a worktree (origin 'dag',
  // correlationId = attemptId) — the worktree path becomes the subagent cwd;
  // the worktree id is stamped into attempt.dispatched for the merge.
  // the merge task: enqueues each worktree-declaring succeeded upstream
  // (origin 'dag') then drains the integration branch —
  //   succeeded/no_changes → task succeeded (outputs: integratedCommit);
  //   conflicted → task blocked(merge_conflicted), waiting_on:'external',
  //                next_hint:'… worktree_queue resolve/retry, then
  //                           dag_control retry_task'
worktree_queue(action:'resolve', …)          // the dsh-worktrees side, manual
dag_control(run_id, action:'retry_task', task_id:'integrate')
dag_tick(run_id) ×N
```

**Status**: the provider-side service face **is implemented** in
[dsh-worktrees](https://github.com/Luck9Star/dsh-worktrees) — its `apply()`
calls `ctx.provide('worktreesEngine', …)` over the SAME service/queue
singletons its tools use (`lib/engine-face.js`), exposing exactly the
`getMergeQueue()` / `getWorktreeService()` pair this plugin's seam admits,
with the four-key enqueue and the five-state `DrainOutcome` drain. With both
plugins installed the composition works end to end; when dsh-worktrees is
absent (or a host ctx has no service face), a spec containing worktree or
merge tasks fails those nodes **loud** with permanent
`dag.worktrees_unavailable` at dispatch; **agent-only DAGs are completely
unaffected**.

**One real prerequisite for worktree isolation** (verified behavior, not a
guess): the subagent `request.cwd` this plugin forwards is honored by the
runtime **only after** [dsh-plugin-subagents](https://github.com/Luck9Star/dsh-plugin-subagents)
is installed and its `patches/install.sh` has been run. The official
`SubagentStartRequest` has no `cwd` field, and neither rc.6 nor rc.7 of the
unpatched official runtime forwards a per-call cwd into the child session
meta — without the patches, a worktree task's subagent silently inherits the
parent's cwd and writes into the wrong tree. The two patches apply verbatim
onto rc.7 anchors (verified), and **must be re-run after every dsh upgrade**
(upgrades reinstall the pristine runtime). See
[dsh-plugin-subagents](https://github.com/Luck9Star/dsh-plugin-subagents)
for the patch set, its installer, and its doctor check.

## Install

Three steps (family convention):

```sh
# 1. Install the plugin (reconcile appends the bundle layer — one insert
#    row, id 'dag'; no official rows are disabled: dag_* are new names)
dsh plugin --profile web add dsh-dag-orchestrator      # or: add <local path>

# 2. [for local checkouts] dsh-tools single-instance links (peer discipline)
cd dsh-dag-orchestrator && npm install && npm run setup:peer

# 3. Restart dsh and open a NEW session
dsh --profile web
```

- **Step 2 is mandatory for local development installs.**
  `@deepseek-ai/{cordis,dsh-tools,dsh-subagent}` are peerDependencies. npm
  ≥7 auto-installs peers as REAL directories (a second physical copy),
  which breaks the harness's single-instance invariants — a second
  `dsh-tools` copy registers its tool-runtime scheduler under a second
  module-level Symbol, and every tool call then dies with
  `Cannot read properties of undefined (reading 'prepare')`.
  `npm run setup:peer` (scripts/link-harness-dsh-tools.sh) symlinks all
  three peers to the copy the running harness uses. It resolves the live
  root dynamically (`command -v dsh` → realpath → walk up to `node_modules`,
  or the `DSH_HARNESS_ROOT` override — never hardcoded cache paths) and
  fails loud when it cannot. Re-run it after every `npm install` here and
  after a dsh upgrade. The plugin also self-checks at apply time and points
  at this fix when a second instance is detected.
- **Requirements**: Node **≥ 22.13** (`node:sqlite`; enforced by
  `engines` — the dynamic `import('node:sqlite')` fails loud, never
  silently degrades). The host itself satisfies this.
- **No mutual exclusion**: `dag_*` are new names — this plugin coexists with
  [dsh-plugin-subagents](https://github.com/Luck9Star/dsh-plugin-subagents)
  (the execution layer) and
  [dsh-worktrees](https://github.com/Luck9Star/dsh-worktrees) (the M3
  composition face), and both are recommended company. For `worktree:` task
  isolation, also run dsh-plugin-subagents' `patches/install.sh` (see
  [④ Deep composition](#④-deep-composition-with-dsh-worktrees-m3)) —
  `request.cwd` forwarding is a patch-level capability of that plugin.
- Config overrides go on the profile-layer row (same id), see
  [Configuration](#configuration).

## Configuration

Config lives on the `dag` insert row `cordis.patch.yml` contributes.
Validation is zod-strict — unknown or misspelled keys fail loudly at apply
time (`dag.config_invalid`).

| Key | Type | Default | Description |
|---|---|---|---|
| `register.plan` … `register.approve` | boolean | `true` | per-tool registration switches (plan / status / tick / control / approve) |
| `dbPath` | string | `~/.dsh/dag-orchestrator/dag.db` | sqlite database path (leading `~` expanded; `:memory:` supported for tests) |
| `defaultMaxRunningAgents` | integer 1..32 | `4` | per-run subagent concurrency cap when the spec omits `limits.maxRunningAgents` |
| `defaultQueueCapacity` | integer 1..1024 | `16` | ready-queue capacity when the spec omits `limits.queueCapacity` (overflow stays ready, re-evaluated next round) |
| `inputInlineLimitBytes` | positive integer | `32768` | per-input inline cap in the assembled prompt; over the cap fails loud permanent `dag.input_too_large` (never silently truncated) |
| `autoTickMs` | integer ≥ 0 | `0` | optional Timer interval for the no-dispatch reconcile (OFF by default; see the lifecycle model) |
| `allowedRoots` | string[] | `[]` | extra roots the dispatch-time cwd gate admits (in addition to the run's base cwd subtree) |
| `requireWorkspaceRegistration` | boolean | `false` | `true` fails CLOSED for every explicit `task.cwd`: no workspace-registry channel exists yet, and a switch demanding a registry the host cannot consult must not degrade into a no-op |

## The WorkflowSpec

`dag_plan`'s `spec` parameter is a JSON document, validated STRICTLY by
`lib/spec-validate.js` (zod strictObject at every level + structural rules).
Unknown keys, cycles, dangling dependencies, unreachable inputs, kind-matrix
violations — all rejected loud with stable `dag.*` codes and precise paths
(`tasks[3].dependsOn[0].taskId`), aggregated into one error. Nothing is ever
guessed or degraded.

```jsonc
{
  "version": 1,
  "name": "refactor-auth",
  "description": "Parallel refactor with review gate and integration",
  "project": { "root": "/abs/repo", "baseRef": "HEAD" },   // root defaults to the session cwd
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

Shape highlights (the full grammar lives in `lib/spec-validate.js`):

- **Task kinds**: `agent` (one subagent delegation), `approval` (a gate; no
  delegation fields, no `retry.maxAttempts > 1`), `merge` (M3; requires ≥1
  succeeded-upstream that declares a `worktree`, enforced at plan time).
  `worktree.task` slugs are unique across the spec (`dag.worktree_slug_conflict`)
  — a slug names ONE task's worktree, never a sharing primitive.
- **Per-task delegation fields** (flat, mirroring the `subagent` tool's
  parameter names): `backend` (`native|spawn|fork`; `native` is an alias
  of `spawn` — the harness registers only the two in-process provider
  names, and the executor maps the alias at dispatch so a `native` task
  never hits `NO_PROVIDER`), `model`, `provider`, `persona`, `toolFilter`,
  `cwd`, `maxTokens`, `maxDepth`, `delegation`. `cwd` passes the dispatch
  gate (absolute, existing, realpath inside base-cwd ∪ `allowedRoots`) and
  is forwarded as `request.cwd` — which the official unpatched runtime
  does NOT honor (see [④ Deep composition](#④-deep-composition-with-dsh-worktrees-m3)):
  install dsh-plugin-subagents and run its `patches/install.sh` to make
  per-call cwd effective.
- **toolFilter floor (red line 5)**: every task subagent dispatches with
  `deny [dag_plan, dag_status, dag_tick, dag_control, dag_approve,
  subagent, subagent_fork]` as the structural floor. The floor can only be
  NARROWED: spec `deny` appends, `allow` passes through but deny stays,
  and `delegation: true` removes only the two `subagent*` names. Entries
  for tools this deployment does not register (a `dag_*` switch off in
  `register`, or the two delegation tools on a host without the subagents
  plugin) are trimmed from the filter at dispatch — the harness's
  `tools.restrict()` rejects unknown names, and denying an unregistered
  tool is vacuous anyway. A REGISTERED `dag_*` tool can never be lifted.
  Spec-authored tool names should be checked against the tools this host
  actually registers — on a host where the tool registry cannot be probed,
  an unregistered name may error at subagent creation time.
- **Dependencies**: `{taskId, condition: succeeded|completed}` — no
  expression language. Optional `gate: {artifact, expect, value?}` (T18):
  a finite five-operator boolean check (`exists | not_exists | contains |
  not_contains | equals`) over an upstream output — not a scripting surface.
- **Inputs/outputs**: `inputs: ["task://<producer>/<name>"]` resolve against
  the outputs table; a producer must be a direct/transitive upstream
  declaring that output; at most ONE output per task (its ObjectJsonSchema
  is passed verbatim as the subagent's `outputSchema`).
- **verify as a contract gate** (no verify executor): a task declaring
  `verify: {expectOutput, expectStatus}` only reaches `succeeded` when THIS
  attempt produced that output with `value.status === expectStatus`; a miss
  synthesizes permanent `dag.verify_gate_failed`. No verify declaration →
  evidence `none_declared`, the gate is skipped. The gate applies to
  agent tasks (at harvest) AND merge tasks (on the merge executor's
  success path) — for a merge, the receipt is the integratedCommit output
  and its status view is the `integratedCommit` itself, so a verify block
  pinning the expected commit gates the integration node.
- **Retries**: `retry: {maxAttempts, backoffMs, maxBackoffMs, jitterRatio,
  retryOn}` — exponential backoff + jitter, `retryOn` filters by policy key
  (`transient_network | permanent | internal`; timeout-class failures retry
  under `transient_network`).
- **Bridge boundary (structural, O2)**: the `permission_mode` /
  `reasoning_effort` keys and any `backend` outside `native|spawn|fork` are
  rejected with `dag.bridge_unsupported` — this plugin binds native
  in-process subagents only; bridge delegation with settings is a future
  seam (see the subagents plugin's engine-level dispatch seam), never a
  silently-ignored parameter.

## The lifecycle model (dies with the host)

- **`dag_tick` is the pump.** One call = a bounded multi-round reconcile:
  reconcile approvals → promote ready → admission gate → build queue →
  dispatch loop (claim + subagent start, outside any transaction) → harvest
  settled attempts (one transaction per attempt — never batched) →
  propagate failures → finalize the run → drain pauses. When a round makes
  no progress and attempts are in flight, the call may wait for them to
  settle, bounded by a WHOLE-CALL `settle_ms` budget (never reset per
  round; plus a two-straight-zero-settle-rounds guard and the `max_rounds`
  clamp) — so one tick can never self-spin.
- **Dispatch happens only inside a tool exec.** Every delegation hangs off
  the live Agent that called the tool (the pumping agent) — a Timer context
  has no live agent, and caching an Agent handle across the host lifetime
  would violate the dies-with-host model. Cross-session resumption is
  natural: session A plans, session B ticks, the work continues.
- **`autoTickMs` (default 0 = off) is the honest Timer.** When > 0, a
  `setInterval` effect runs the **no-dispatch reconcile** on every
  non-terminal run: approval promotions, harvesting settled attempts,
  ready-promotion, failure propagation, finalization, pause-draining —
  everything except dispatch. A promoted-ready task then simply waits for
  the next real `dag_tick` to dispatch it (the documented cost of the
  boundary). Timer callbacks never throw into the host.
- **Crash reconciliation runs before tool registration.** On `apply()` the
  event chain is re-verified (a mismatch parks that run `failed` +
  `recovery.chain_broken`; other runs continue), never-dispatched claims
  auto-retry immediately, dispatched-orphaned attempts park `orphaned` with
  a human-action event — the bounded policy: the only automatic action is
  retrying what provably never ran; everything else routes to a human.
- **Teardown** disposes every in-flight subagent, sweeps the engine, closes
  the store (idempotent), and `apply()` returns `undefined` (the Cordis
  loader contract).

## dsh-worktrees composition (M3)

See sequence ④ above. The composition seam is **opportunistic**: the plugin
probes `ctx.get('worktreesEngine')` once at apply (for an honest
availability log line) and re-probes on every use — it does NOT `inject`
(an optional seam must not block plugin load until a dependency appears).
A face is admitted only when it exposes both `getMergeQueue()` and
`getWorktreeService()` as functions; anything else is treated exactly like
absence. The DAG never creates a second merge queue over the same
worktrees state (their §10 forbids it) and never deletes worktrees — the
worktree lifecycle belongs to the worktrees plugin; a DAG attempt's
terminal state leaves the worktree for merge collection or manual cleanup.
The provider side is live:
[dsh-worktrees](https://github.com/Luck9Star/dsh-worktrees) `apply()` calls
`ctx.provide('worktreesEngine', …)` over the same service/queue singletons
its tools use — with both plugins installed, worktree tasks and merge nodes
work end to end (the consumer contract is the `WorktreesEngineFace` JSDoc
in `lib/worktrees-seam.js`, which the provider's `lib/engine-face.js`
implements). When the face is absent, merge/worktree tasks fail loud
permanent `dag.worktrees_unavailable` and agent-only DAGs are unaffected.

**`request.cwd` needs dsh-plugin-subagents' patches.** The worktree path
this plugin resolves becomes the subagent's `request.cwd` — but the
official runtime's `SubagentStartRequest` carries no `cwd` field, and the
unpatched rc.6/rc.7 runtime simply drops it (the child inherits the parent
cwd). The verified remediation: install
[dsh-plugin-subagents](https://github.com/Luck9Star/dsh-plugin-subagents)
and run its `patches/install.sh` — its two minimal patches forward
`request.cwd` into the child session's creation meta, and both apply
verbatim onto rc.7 anchors. Re-run the installer after every dsh upgrade
(an upgrade reinstalls the pristine runtime), which is also why
[dsh-plugin-subagents](https://github.com/Luck9Star/dsh-plugin-subagents)
is recommended company for any DAG that uses `worktree:` tasks or explicit
`task.cwd`.
Worktree reuse is scoped to the SAME task's re-dispatch (`retry_task` /
retry policy — DESIGN §11.3): each `worktree.task` slug is unique across
the spec (`dag.worktree_slug_conflict` at plan time), and at dispatch the
active record is reused only when its `correlationId` belongs to that
task's own attempt history — a record that cannot prove ownership (or
belongs to another task) falls through to create, which reports an
occupied slug loud (`dag.worktree_create_failed`) instead of silently
sharing a checkout. When you DO compose them for real,
align the two plugins' roots: dsh-worktrees' `worktreeRoot` (default
`~/.dsh/worktrees/` — OUTSIDE the repo subtree) must sit inside this
plugin's cwd-gate admission (`config.allowedRoots` or the run's base-cwd
subtree); otherwise an engine-provided worktree path is rejected by this
plugin's cwd gate and the task spins on transient
`dag.worktree_create_failed`. Either point dsh-worktrees' `worktreeRoot`
inside the repo, or add that root to `allowedRoots` here.

## Multi-instance boundary (one database, one host)

WAL + `busy_timeout = 5000` keep the file safe when another process
happens to read, but **cross-instance concurrent writes are NOT supported**:
two dsh hosts ticking the same database produce loud busy-timeout failures
on the loser (acceptable: no corruption, no silence). Operate each DAG
database through exactly ONE DSH host. The database file is created
owner-only (`0600`, exclusive `wx`, parent directories `0700`) and guarded
by an `application_id` magic — a foreign or wrong-version file refuses to
open.

## Development

```bash
npm install
npm run setup:peer     # symlink the running harness's @deepseek-ai peers (above)
npm test               # node:test — 514 cases; fakes only, zero network/CLI/model
npm run lint           # node --check every module + the discipline audits (below)
```

The suite must never require a real CLI, a key, a network, or a live
harness — it runs green on a bare runner with fake executor/store/ctx
(peers resolve from the npm registry per the lockfile; CI does exactly
this). CI (`.github/workflows/ci.yml`) runs macOS/Ubuntu × Node
22.13/24: `npm ci` → `npm run lint` → `npm test`. Windows is deliberately
deferred until `node:sqlite` is validated on a Windows CI runner.

`npm run lint` mechanically enforces the repo's core disciplines:
`node:sqlite` / `DatabaseSync` may appear ONLY in `lib/dag-store.js` (the
single persistence outlet); `db.exec(` arguments are allowlisted to
module-constant PRAGMA/DDL (user data never enters SQL strings); imports
from `@deepseek-ai/dsh-subagent` are a pure-function whitelist
(`assertSubagentMaxDepth`, only in `lib/executor.js`).

## Security

Task prompts are **data**, the DAG control plane is denied to task
subagents by default, explicit task cwds pass a fail-closed realpath gate,
and bridge escalation fields are rejected structurally. See
[SECURITY.md](SECURITY.md).

## License

MIT
