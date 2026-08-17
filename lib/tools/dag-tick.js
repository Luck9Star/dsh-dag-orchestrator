// dag_tick — one bounded reconcile pump call (DESIGN §8.3, TASKS.md T08).
//
// The core driver: passes run_id + the caller's max_rounds / settle_ms
// straight to engine.tick and returns the tickSummary verbatim (the engine
// already emits the §8.3 shape, json-safe). Unknown run → loud
// `dag_tick: dag.run_not_found — <runId>` BEFORE the engine call, so the
// message keeps the tool-layer error contract even if the engine wording
// changes.

import { defineTool } from '@deepseek-ai/dsh-tools'

/**
 * Register the dag_tick tool. deps = { engine, store, config }. Returns
 * undefined.
 *
 * @param {{tools: {register: Function}}} ctx plugin context (fake in tests)
 * @param {{engine: object, store: object, config?: object}} deps
 */
export function registerDagTick(ctx, deps) {
  const { engine, store } = deps
  if (!engine || typeof engine.tick !== 'function') {
    throw new Error('dag_tick: requires deps.engine with tick')
  }
  if (!store || typeof store.findRun !== 'function') {
    throw new Error('dag_tick: requires deps.store with findRun')
  }

  ctx.tools.register(defineTool({
    name: 'dag_tick',
    description: 'Advance one DAG run: a bounded multi-round reconcile (promote ready tasks, dispatch subagents up to maxRunningAgents, harvest settled attempts, propagate failures, finalize the run when done). Returns the tick summary — run_state, per-round counters, in_flight attempts, and waiting_on, which tells you what to do next (in_flight_attempts → tick again after work settles; approval → ask the user, then dag_approve; nothing + terminal → done).',
    parameters: {
      run_id: { type: 'string', required: true, description: 'The run to advance.' },
      max_rounds: { type: 'integer', description: 'Inner reconcile rounds per call (default 4, max 16).' },
      settle_ms: { type: 'integer', description: 'Bounded wait for in-flight attempts to settle when a round makes no progress (default 10000, max 60000).' },
    },
    output: {
      // §8.3 tickSummary — produced by the engine; an open object root
      // avoids duplicating that contract here (json-safe either way).
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 2) }],
    },
    isConcurrencySafe: () => false,
    async execute(args, exec) {
      if (store.findRun(args.run_id) === null) {
        const failure = new Error(`dag_tick: dag.run_not_found — ${args.run_id}`)
        failure.code = 'dag.run_not_found'
        throw failure
      }
      return engine.tick(args.run_id, {
        maxRounds: args.max_rounds,
        settleMs: args.settle_ms,
        execAgent: exec?.agent,
      })
    },
  }))
}
