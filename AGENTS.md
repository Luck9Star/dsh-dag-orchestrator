# AGENTS.md

Guidance for AI agents (DeepSeek Harness agents, Claude Code, Codex, …)
working in this repository.

## What this is

`dsh-dag-orchestrator` — a DeepSeek Harness (Cordis) plugin for resumable
multi-task parallel DAG orchestration: five `dag_*` tools over a
tick-driven reconcile engine, `node:sqlite` persistence with a per-run
event hash chain, crash reconciliation on apply, and one DAG task node =
one programmatic native-subagent delegation (`ctx.subagents.start`). The
scheduler core is a narrowing port of task-weaver's `packages/scheduler/`;
the design record is [docs/DESIGN.md](docs/DESIGN.md) and the task
breakdown is [docs/TASKS.md](docs/TASKS.md).

## Commands

```bash
npm install        # install dependencies
npm run setup:peer # symlink the RUNNING harness's @deepseek-ai peers
npm test           # node:test suite (493 cases; fakes only)
npm run lint       # node --check every module + the discipline audits
```

Never add a test that requires a real CLI, a key, a network, a live model,
or a live harness — the suite must stay green on a bare runner. When
migrating code from task-weaver, do not weaken migrated assertions; new
cases add coverage, they don't change old semantics.

## Repo layout

```
cordis.patch.yml      # bundle patch: ONE insert row (id: dag) — no disables
lib/
  index.js            # apply(): config, peer self-check, store, reconcile, assembly, tools
  config.js           # zod strict config (§T09 table; unknown keys fail loudly)
  dag-store.js        # THE ONLY sqlite outlet: 7 tables, tx(), CAS, event hash chain
  spec-validate.js    # strict WorkflowSpec validation + specHash (stable dag.* codes)
  engine.js           # the state machine: oneRound 10 steps, tick loop, control, status
  executor.js         # native subagent binding: dispatch/harvest/abort, filter floor
  admission.js        # in-memory slots + concurrencyKey session mutex
  ready-evaluator.js  # evaluateReady/upstreamSatisfies/isReadySource/evaluateGate (ported)
  bounded-queue.js    # 5-level priority ready queue (ported, unchanged)
  critical-path.js    # criticalPathDepth (ported, unchanged)
  terminal-commit.js  # per-attempt terminal commit + retry branch (ported semantics)
  recovery.js         # apply-time crash reconciliation (chain verify + orphan taxonomy)
  verify-gate.js      # evaluateVerifyGate (ported; receipt source = outputs table)
  cwd-gate.js         # red-line-9 dispatch-time cwd gate (realpath containment)
  reflect.js          # hand-written promise-state wrappers (no Promise.withResolvers)
  worktrees-seam.js   # the opportunistic dsh-worktrees engine seam (WorktreesEngineFace JSDoc)
  executors/merge.js  # the merge-kind executor (DrainOutcome five-state mapping)
  tools/              # one module per model-facing tool (dag_plan/status/tick/control/approve)
scripts/              # link-harness-dsh-tools.sh (peer symlinks), lint.js (discipline audit)
test/                 # node:test suite (fakes only — zero network, zero real CLI)
docs/DESIGN.md        # the architecture record — read before changing semantics
docs/TASKS.md         # task breakdown and acceptance criteria
```

## Safety red lines (non-negotiable)

All twelve are binding (DESIGN §10); every one is mechanically or
structurally enforced. If a change needs to cross one, the design doc
changes first.

1. **Spec validation is strict.** zod strictObject at every level plus the
   structural rules (acyclic, references resolve, kind matrix). Unknown
   keys, unknown dependencies, unknown conditions are loud rejections
   (`dag.*` codes) — never a guess, never a degraded execution.
2. **State changes only through engine transactions.** No tool ever writes
   a projection table: `dag_approve` writes the approvals row + its event
   only, promotion belongs to the next tick. Every projection change
   happens inside `store.tx()` with its event.
3. **Projection + event + hash in the same transaction; the chain is
   verified on load.** A mismatch parks that run failed
   (`recovery.chain_broken`) — fail closed, per run.
4. **Crash reconciliation runs before tool registration.** apply() order is
   config → peer self-check → store open → reconcile → assembly → tools.
   No `dag_*` call may observe a pre-reconciliation state.
5. **Prompts are data.** Upstream outputs inline between boundary markers;
   spec.prompt is a string, never parsed or executed. Every task subagent
   dispatches with the DEFAULT_TASK_FILTER deny floor: `dag_*` control
   tools can NEVER be lifted; `subagent`/`subagent_fork` are removed only
   by an explicit `delegation: true` (and nothing else may be removed —
   spec `deny` only APPENDS). Approval decisions come only from the
   orchestration conversation through `dag_approve`.
6. **Permissions never escalate.** Native backends only: `permission_mode`
   / `reasoning_effort` keys and any backend outside `native|spawn|fork`
   are loud `dag.bridge_unsupported` rejections at validation time. If a
   bridge seam ever opens, it must reuse the subagents plugin's PERM_RANK
   ceiling — this rule is the O2 precondition.
7. **Persistence is atomic and owned.** All writes inside
   `store.tx()` (BEGIN IMMEDIATE); parameterized statements only — user
   data never enters an SQL string (`db.exec(` is allowlisted to
   module-constant PRAGMA/DDL, lint-enforced); the file is created 0600
   exclusive `wx` with the `application_id` ownership guard; no partial
   JSON writes.
8. **Zero network, zero real CLI in tests.** The plugin itself makes no
   network requests (subagent tool use is governed by the host sandbox).
   Tests are `node:test` with fake executor/store/ctx — nothing that needs
   a key, a CLI, or a live harness.
9. **The cwd gate fails closed.** An explicit `task.cwd` must be absolute,
   exist, and realpath inside the base-cwd subtree ∪ `config.allowedRoots`
   (judged on realpaths of BOTH ends). No any-root switch.
   `requireWorkspaceRegistration: true` denies every explicit cwd (no
   registry channel exists — the switch must not degrade into a no-op).
10. **Timeouts and resource bounds are enforced.** Every attempt gets an
    AbortController + timer (cleared in `finally`); `maxRunningAgents`
    1..32 and `queueCapacity` 1..1024 per the limits table; `max_rounds`
    and `settle_ms` are clamped (a tick can never self-spin; the settle
    budget is whole-call, never reset per round).
11. **Inputs are bounded, never truncated.** One input over
    `inputInlineLimitBytes` (default 32 KiB) is a loud
    `dag.input_too_large` failure — reject, don't silently cut.
12. **Config is zod-strict and the runtime is pinned.** Unknown config keys
    fail loud at apply; `engines >= 22.13`; a failed dynamic
    `import('node:sqlite')` refuses to start (no silent JSON fallback);
    `apply()` returns `undefined` (the Cordis loader contract); the
    dsh-tools dual-instance self-check runs at apply with dedupe-fix
    guidance.

## Import discipline

- **sqlite has exactly one outlet**: `node:sqlite` / `DatabaseSync` may
  appear ONLY in `lib/dag-store.js`. Every other module goes through the
  store's methods or `tx()`. `scripts/lint.js` enforces this statically.
- **`@deepseek-ai/dsh-subagent` imports are a pure-function whitelist**:
  exactly one member, `assertSubagentMaxDepth`, imported by exactly one
  module, `lib/executor.js`, asserted before every dispatch. Adding any
  other import or member needs the whitelist (and this file) updated
  first. Lint-enforced.
- `dsh-worktrees` is reached ONLY through the Cordis service face
  (`ctx.get('worktreesEngine')` via `lib/worktrees-seam.js`) — never by
  importing its modules, and never by building a second merge queue over
  its state.

## Conventions

- Update `README.md` and `README.zh.md` **together** — the two must stay
  section-for-section aligned whenever the config surface, tools, install
  flow, composition, or lifecycle model change.
- Add a `CHANGELOG.md` entry for every user-visible change.
- Code ported from task-weaver stays line-for-line / semantics-equivalent
  except where DESIGN explicitly requires a rename or narrowing; preserve
  original comment semantics.
- Keep `lib/index.js` thin (assembly only); tools live in `lib/tools/`,
  one module per tool; engines are constructor-injected (`{ store,
  executor, admission, config }`) and never import host services.
- Tool outputs are built with conditional expansion — no undefined-valued
  keys (the host's lossless-JSON snapshot gate rejects them).
- Every error surfaced to the model carries a stable `dag.*` code on the
  thrown Error (`dag_plan: <code> — <detail>` shape at the tool layer).
