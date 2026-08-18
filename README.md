# dsh-dag-orchestrator

**English** | [简体中文](README.zh.md)

> Runs on DeepSeek Harness (dsh) `0.1.0-rc.6` / `0.1.0-rc.7` · Node ≥ 22.13 (needs `node:sqlite`) · MIT

Turn a multi-step job into a **parallel task graph that survives restarts**.

You describe the job once as a JSON list of tasks and their dependencies
(a DAG), hand it to `dag_plan`, and the plugin does the rest: tasks with no
pending dependencies are dispatched **in parallel** (each task = one
subagent delegation), finished outputs flow to downstream tasks, failures
retry or propagate, human-approval gates pause until you decide, and the run
finalizes when everything is done. Progress lives in a local SQLite
database — dsh can crash or restart mid-run and the DAG picks up where it
left off.

## Why

Two problems with "just delegate the whole job to one subagent":

1. **It runs serially.** One subagent does step 1, then step 2, then step 3 —
   even when the steps are independent and could run three-at-a-time.
2. **Nothing survives a restart.** A host restart mid-job loses the plot:
   half-finished work, no record of what succeeded, start over.

This plugin fixes both: parallelism (bounded by `maxRunningAgents`) and
durability (every state change is committed to SQLite with a hash-chained
event log; on the next start, crashed runs are reconciled and can resume).

## The five tools

| Tool | What it does |
| --- | --- |
| `dag_plan` | Submit a task graph (JSON spec). Returns a `run_id` and does the first dispatch round. |
| `dag_tick` | Pump the run: dispatch what is ready, harvest what finished, propagate failures. Call repeatedly until terminal. |
| `dag_status` | Inspect a run: summary / per-task / per-attempt / full event log. |
| `dag_control` | Pause / resume / stop the run; manually retry or cancel a task. |
| `dag_approve` | Answer an approval gate (`approve` / `reject`); the next tick proceeds accordingly. |

## Install

```sh
# 1. Get the repo and link it to your running dsh's internal packages
git clone https://github.com/Luck9Star/dsh-dag-orchestrator
cd dsh-dag-orchestrator
npm install
npm run setup:peer        # avoids a second copy of dsh-tools (tool calls crash without this)

# 2. Install into your dsh profile and restart
dsh plugin --profile web add "$(pwd)"
dsh --profile web         # open a NEW session
```

**Expected result:** a new session exposes the `dag_plan`, `dag_status`,
`dag_tick`, `dag_control`, `dag_approve` tools. The database appears at
`~/.dsh/dag-orchestrator/dag.db` after the first `dag_plan`.

## Quick start

A tiny spec — analyze first, then two implementation tasks in parallel:

```jsonc
{
  "version": 1,
  "name": "refactor-auth",
  "limits": { "maxRunningAgents": 3, "queueCapacity": 16 },
  "tasks": [
    { "id": "analyze", "kind": "agent", "prompt": "Read the auth module and produce a summary.",
      "outputs": [{ "name": "analysis",
        "schema": { "type": "object", "additionalProperties": false,
                    "properties": { "summary": { "type": "string" } },
                    "required": ["summary"] } }] },
    { "id": "impl-core", "kind": "agent",
      "dependsOn": [{ "taskId": "analyze", "condition": "succeeded" }],
      "inputs": ["task://analyze/analysis"],          // upstream output inlined into the prompt
      "prompt": "Implement the core change. Upstream analysis: ${inputs}" },
    { "id": "impl-docs", "kind": "agent",
      "dependsOn": [{ "taskId": "analyze", "condition": "succeeded" }],
      "prompt": "Update the docs." }
  ]
}
```

Then drive it:

```jsonc
dag_plan({ spec })            // → { run_id, task_count: 3, initial_tick: { dispatched: 1, … } }
dag_tick({ run_id })          // → { waiting_on: "in_flight_attempts", … } — call again
dag_tick({ run_id })          // → { run_state: "succeeded", waiting_on: "nothing" }
dag_status({ run_id, detail: "tasks" })   // final check, per-task outcomes + outputs
```

`dag_tick` tells you what it is waiting on: `in_flight_attempts` (keep
ticking), `approval` (a gate wants a human), `external`, or `nothing` (the
run is terminal).

**Approval gates:** give a task `"kind": "approval"` with an `approval`
block. When the run reaches it, `dag_tick` returns `waiting_on: "approval"`
with the gate's prompt; relay it to the human, then
`dag_approve({ run_id, task_id, decision: "approve" | "reject" })` and tick
again.

## Crash recovery, concretely

- The host dies mid-run → on the next dsh start, before any tool is
  registered, the plugin re-verifies every run's event hash-chain and
  reconciles: claims that were never dispatched are auto-failed and
  retried; attempts dispatched but unresolved are parked with a
  `recovery.action_requested` event pointing at the child session.
- You then call `dag_status(detail: "attempts")`, optionally
  `dag_control(action: "retry_task")`, and `dag_tick` — the run continues.
- Cross-session works too: session A plans, session B ticks, the work
  continues. There is no background daemon — the plugin lives and dies with
  the dsh process, and `dag_tick` is the pump.

## Task spec — the fields that matter

Full grammar: [docs/DESIGN.md](docs/DESIGN.md). Spec is JSON (not YAML),
strictly validated — unknown fields, cycles, dangling references, and
misused per-kind fields all fail with a specific `dag.*` error code.

| Field | Meaning |
| --- | --- |
| `tasks[].id` / `kind` | Unique id; `agent` (subagent task), `approval` (human gate), `merge` (integration node, needs dsh-worktrees). |
| `tasks[].prompt` | The task text the subagent gets. |
| `tasks[].dependsOn` | `[{ taskId, condition: "succeeded" \| "completed", gate? }]` — optional artifact gates (`exists`, `contains`, …). |
| `tasks[].inputs` | `["task://<producer>/<output>"]` — upstream structured outputs inlined into this task's prompt. |
| `tasks[].outputs` | Up to one named output with a JSON schema; the subagent's reply is validated against it before downstream tasks run. |
| `tasks[].model` / `provider` / `persona` / `toolFilter` / `cwd` / `maxTokens` | Per-task delegation settings, same names as the `subagent` tool. |
| `tasks[].retry` | `{ maxAttempts, backoffMs, maxBackoffMs, retryOn: ["transient_network" \| "permanent" \| "internal"] }`. |
| `tasks[].timeoutMs` / `priority` / `failurePolicy` | Timeout (default 30 min); queue priority; `block_downstream` (default) or `isolate`. |
| `tasks[].worktree` / `merge` | Isolated git worktree for this task / merge node target branch — requires [dsh-worktrees](https://github.com/Luck9Star/dsh-worktrees). |

**Anti-injection by default:** every task subagent is dispatched with the
`dag_*` and `subagent*` tools denied — a task agent can never drive the DAG
it is part of. Upstream outputs are inlined between data markers, never as
instructions.

## Configuration

Optional — everything below has a working default. Keys live on the plugin's
row in your profile's `cordis.patch.yml`; unknown keys fail loudly at startup.

| Key | Default | Meaning |
| --- | --- | --- |
| `dbPath` | `~/.dsh/dag-orchestrator/dag.db` | SQLite database location (`:memory:` supported). One database serves one dsh host. |
| `defaultMaxRunningAgents` | `4` | Default parallelism cap (spec can override, max 32). |
| `defaultQueueCapacity` | `16` | Default waiting-queue capacity. |
| `autoTickMs` | `0` (off) | Auto-tick interval; without it you (or the model) call `dag_tick` manually. |
| `allowedRoots` | `[]` | Extra repo roots tasks may operate in (needed if worktrees live outside the session cwd — see below). |
| `requireWorkspaceRegistration` | `false` | Restrict repos to registered workspaces. |
| `inputInlineLimitBytes` | `32768` | Cap on inlined upstream outputs. |
| `register.*` | `true` | Per-tool switches. |

## Works well with

- [dsh-plugin-subagents](https://github.com/Luck9Star/dsh-plugin-subagents)
  — **needed for per-task `cwd`** (isolated worktrees). The stock harness
  silently drops `cwd`; install that plugin *and run its
  `patches/install.sh`*, re-running it after every dsh upgrade.
- [dsh-worktrees](https://github.com/Luck9Star/dsh-worktrees) — enables
  `worktree:` task isolation and `merge` nodes. One gotcha: its default
  worktree root (`~/.dsh/worktrees/`) is outside your repo — add that path
  to `allowedRoots` here or worktree tasks will fail to create.

## Boundaries

- **One database, one host.** Two dsh instances writing the same `dag.db`
  are not supported — route a database through a single host.
- **Dies with the host.** No daemon, no scheduler process; nothing runs
  unless dsh is running and someone ticks (or `autoTickMs` is set).

## Troubleshooting

| Symptom | Cause → fix |
| --- | --- |
| Every tool call dies with `Cannot read properties of undefined (reading 'prepare')` | Two physical copies of `dsh-tools`. Re-run `npm run setup:peer` in this repo. |
| `dag.worktrees_unavailable` on a worktree/merge task | dsh-worktrees is not installed (or its engine face not loaded). Install it, or drop those tasks. |
| Worktree tasks spin on `dag.worktree_create_failed` | The worktree root is outside `allowedRoots`. Add `~/.dsh/worktrees/` (or your configured root) to `allowedRoots`. |
| A task's subagent wrote into the wrong directory | The harness dropped `cwd` — install dsh-plugin-subagents and run its `patches/install.sh`. |
| Run parked after a crash with `recovery.action_requested` | Expected. `dag_status(detail: "attempts")`, decide `retry_task` or move on, then `dag_tick`. |

## Development

```sh
npm install && npm run setup:peer   # link the running harness's peers
npm test                            # node --test, fakes only — no network, CLI, or live model
npm run lint
```

Design record: [docs/DESIGN.md](docs/DESIGN.md).

## References & credits

- **task-weaver** (`packages/scheduler/`, `recovery-service`) — the
  scheduler core (CAS claims, per-attempt terminal transactions, event hash
  chains, crash reconciliation) is a narrowing port of it.
- **DeepSeek Harness** `ctx.subagents` API — the execution surface; one task
  node is exactly one programmatic subagent delegation.
- **dsh-session-query-sqlite** — the `node:sqlite` discipline (WAL, 0600
  exclusive create, `application_id` ownership guard) follows its precedent.

## Security

See [SECURITY.md](SECURITY.md). The database is created owner-only; the
event chain makes every state change tamper-evident and auditable
(`dag_status(detail: "events")`).

## License

[MIT](LICENSE)
