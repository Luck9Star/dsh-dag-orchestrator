// dag_plan — submit / validate a WorkflowSpec and start the run (DESIGN
// §8.1, TASKS.md T08).
//
// The tool layer is THIN and STATELESS (task discipline): it resolves the
// base cwd, validates the spec STRICTLY through lib/spec-validate.js
// (aggregating every error, `dag_plan: <code> — <path>: <message>` per line),
// enforces the name_exists rule against the store's non-terminal runs, then
// hands everything to the engine — planRun (one tx: run row + pending tasks
// + run.created event) followed by an INLINE FIRST TICK
// (maxRounds:1, settleMs:0) so progress is visible immediately. Zero
// subagent calls and zero direct sqlite happen here (red lines 2/7).
//
// name_exists semantics (M1 decision): same-name lookup filters the store's
// findNonTerminalRuns() by parsed spec_json name. resume=false (default) →
// loud `dag_plan: dag.name_exists — run <id>`; resume=true → reuse the
// existing run (spec_hash from the stored row, fresh one-round tick) — the
// run is by definition non-terminal, so tick keeps driving it.

import { defineTool } from '@deepseek-ai/dsh-tools'

import { validateSpec } from '../spec-validate.js'

/** §8.6 composition sequence ① (pure agent DAG), embedded per DESIGN. */
const COMPOSITION_EXAMPLE = [
  'Typical sequence (pure agent DAG):',
  'dag_plan(spec) → returns run_id + initial_tick {dispatched:3};',
  'then dag_tick(run_id) ×N while waiting_on: in_flight_attempts, until run_state: succeeded;',
  'finally dag_status(detail:"tasks") to verify the end state.',
].join(' ')

/**
 * Resolve the run's base cwd (§8.1 behavior / §4.6): the calling agent's
 * session cwd when present, else process.cwd as the optional-probe
 * degradation (M1: NO workspaceRegistry probing — cwd validation is the
 * dispatch layer's job, config.allowedRoots gates it there; the plan layer
 * only picks the base).
 *
 * @param {object} [exec] tool execution context (exec.agent is the caller)
 * @returns {string}
 */
function resolveBaseCwd(exec) {
  return exec?.agent?.session?.header?.cwd ?? process.cwd()
}

/**
 * Find a NON-terminal run with the same spec name (the name_exists rule).
 * Returns the planRun-shaped handle (camelCase) so the reuse path and the
 * create path flow through identical code below.
 *
 * @param {object} store DagStore handle
 * @param {string} name validated spec name
 * @returns {{runId: string, specHash: string, taskCount: number} | null}
 */
function findSameNameRun(store, name) {
  for (const run of store.findNonTerminalRuns()) {
    if (run.name !== name) continue
    let taskCount = 0
    try {
      taskCount = JSON.parse(run.spec_json).tasks?.length ?? 0
    } catch {
      taskCount = 0
    }
    return { runId: run.run_id, specHash: run.spec_hash, taskCount }
  }
  return null
}

/**
 * Register the dag_plan tool. deps = { engine, store, config } (the store
 * read here is a projection query, not a write — the only writer is the
 * engine). Returns undefined.
 *
 * @param {{tools: {register: Function}}} ctx plugin context (fake in tests)
 * @param {{engine: object, store: object, config?: object}} deps
 */
export function registerDagPlan(ctx, deps) {
  const { engine, store } = deps
  if (!engine || typeof engine.planRun !== 'function' || typeof engine.tick !== 'function') {
    throw new Error('dag_plan: requires deps.engine with planRun and tick')
  }
  if (!store || typeof store.findNonTerminalRuns !== 'function') {
    throw new Error('dag_plan: requires deps.store with findNonTerminalRuns')
  }

  ctx.tools.register(defineTool({
    name: 'dag_plan',
    description: 'Submit a WorkflowSpec: strictly validated (unknown keys rejected, DAG must be acyclic, dependsOn/inputs must resolve), persisted as a new run with all tasks pending, then one inline reconcile tick so progress is visible immediately. Returns run_id, spec_hash, task_count, and the initial tick summary. ' + COMPOSITION_EXAMPLE,
    parameters: {
      spec: {
        type: 'json',
        required: true,
        description: 'WorkflowSpec JSON (see DESIGN §7.2). Validated strictly: unknown keys rejected, DAG must be acyclic, dependsOn/inputs must resolve.',
      },
      resume: {
        type: 'boolean',
        description: 'If a run with the same name exists non-terminal, resume it instead of erroring (default false → loud name_exists).',
      },
    },
    output: {
      // §8.1 shape. initial_tick is a full tickSummary (§8.3) — free-form
      // object (the exact tick shape is the engine's contract; keeping it
      // open here avoids duplicating that schema).
      schema: {
        type: 'object',
        additionalProperties: false,
        properties: {
          kind: { type: 'string', const: 'plan' },
          run_id: { type: 'string', required: true },
          spec_hash: { type: 'string', required: true },
          task_count: { type: 'integer', required: true },
          initial_tick: { type: 'object', additionalProperties: true },
          warnings: { type: 'array', items: { type: 'string' } },
        },
      },
      render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 2) }],
    },
    isConcurrencySafe: () => false,
    async execute(args, exec) {
      // Strict validation FIRST (fail loud before any persistence).
      const validated = validateSpec(args.spec)
      if (!validated.ok) {
        // Aggregate every error: 'dag_plan: <code> — <path>: <message>' per
        // line, first line carries the prefix (§8.1 error contract).
        const lines = validated.errors.map((error) => {
          const where = error.path ? `${error.path}: ` : ''
          return `${error.code} — ${where}${error.message}`
        })
        const failure = new Error(`dag_plan: ${lines.join('\n')}`)
        failure.code = 'dag.schema_invalid'
        failure.errors = validated.errors
        throw failure
      }
      const spec = validated.value

      const baseCwd = resolveBaseCwd(exec)
      const parentSession = exec?.agent?.session?.id
      const execAgent = exec?.agent

      // name_exists gate (M1: filter findNonTerminalRuns by parsed name).
      const existing = findSameNameRun(store, spec.name)
      if (existing !== null && args.resume !== true) {
        const failure = new Error(`dag_plan: dag.name_exists — run ${existing.runId}`)
        failure.code = 'dag.name_exists'
        failure.run_id = existing.runId
        throw failure
      }

      // Reuse or create. On reuse the run is non-terminal by construction,
      // so the inline tick below resumes it with the STORED spec_hash.
      const planned = existing ?? engine.planRun(spec, { baseCwd, parentSession, execAgent })
      // Inline first tick (maxRounds:1, settleMs:0) — progress visible now.
      const initialTick = await engine.tick(planned.runId, { maxRounds: 1, settleMs: 0, execAgent })

      return {
        kind: 'plan',
        run_id: planned.runId,
        spec_hash: planned.specHash,
        task_count: planned.taskCount,
        initial_tick: initialTick,
        warnings: [], // reserved (e.g. future bridge-field guidance)
      }
    },
  }))
}
