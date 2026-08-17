// dag_status — projection query (DESIGN §8.2, TASKS.md T08).
//
// Read-only passthrough to engine.status: the detail depths
// (summary|tasks|attempts|events), the optional task_id filter, and the
// events tail window (limit, default 50) all map 1:1. Omitting run_id asks
// for the all-runs summary (§8.2's "Omit to list all runs" arm). The engine
// is the only reader of the store here — the tool layer stays stateless.

import { defineTool } from '@deepseek-ai/dsh-tools'

/**
 * Register the dag_status tool. deps = { engine, store, config }; only the
 * engine is used. Returns undefined.
 *
 * @param {{tools: {register: Function}}} ctx plugin context (fake in tests)
 * @param {{engine: object, store?: object, config?: object}} deps
 */
export function registerDagStatus(ctx, deps) {
  const { engine } = deps
  if (!engine || typeof engine.status !== 'function') {
    throw new Error('dag_status: requires deps.engine with status')
  }

  ctx.tools.register(defineTool({
    name: 'dag_status',
    description: 'Query DAG run projections: omit run_id for the all-runs summary; with run_id, choose a detail depth — summary (run row + task-state counts), tasks (+ per-task state/attempts/ordinal/blocked reason), attempts (+ per-attempt rows with child session and stop reason), or events (the hash-chain tail window, default 50, filterable by task_id). Read-only; safe to call any time.',
    parameters: {
      run_id: { type: 'string', description: 'Omit to list all runs (summary rows).' },
      detail: {
        type: 'string',
        enum: ['summary', 'tasks', 'attempts', 'events'],
        description: 'Projection depth (default tasks).',
      },
      task_id: { type: 'string', description: 'Filter to one task (detail=attempts/events).' },
      limit: { type: 'integer', description: 'events tail window (default 50).' },
    },
    output: {
      // §8.2 output is depth-dependent (summary rows vs run + tasks vs
      // attempts vs events) — an open object root is the honest shape.
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 2) }],
    },
    isConcurrencySafe: () => true,
    async execute(args) {
      const runId = args.run_id === '' ? undefined : args.run_id
      // Conditional expansion — no undefined-valued keys in the call shape.
      const options = {}
      if (args.detail !== undefined) options.detail = args.detail
      if (args.task_id !== undefined) options.taskId = args.task_id
      if (args.limit !== undefined) options.limit = args.limit
      return engine.status(runId, options)
    },
  }))
}
