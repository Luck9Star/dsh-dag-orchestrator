// dag_control — the run/node control plane (DESIGN §8.4, TASKS.md T11).
//
// THIN and STATELESS like its siblings: the tool validates existence, maps
// the §8.4 argument names onto engine.control's options, and returns the
// control summary verbatim. Every projection change happens inside the
// engine's transactions with its events (red line 2 — tools never write
// state); the ONLY store read here is the not-found pre-check so the error
// keeps the tool-layer contract (`dag_control: dag.run_not_found — <id>`)
// even if the engine wording changes.
//
// Action semantics (DESIGN §8.4):
//   pause       running → pausing (intent=pause; admission closes, in-flight
//               keeps running) → drainToPaused lands paused after the
//               in-flight attempts settle. Idempotent on pausing/paused.
//   resume      paused → running + intent cleared (admission reopens).
//   stop        → cancelling + intent=stop: every NON-running task cancels
//               inside the control tx; in-flight attempts abort right after
//               (outside the tx) and land attempt+task cancelled on the NEXT
//               tick's harvest; finalizeRunIfDone then aggregates the run to
//               cancelled. The abort sweep is idempotent and re-armed per
//               reconcile pass (an attempt dispatched across the stop also
//               gets aborted). STOP NEEDS A dag_tick TO FINISH — control
//               returns the intent-layer state only.
//   retry_task  failed | blocked(upstream_failed) | blocked(
//               merge_conflicted) → retry_wait (immediate) +
//               task.retry_requested manual event. Manual retries are NOT
//               billed against the retryOn budget (explicit human action).
//               The merge_conflicted arm (T17) is the conflicted merge
//               park's ONLY re-run edge — resolve the worktrees queue
//               first (worktree_queue resolve/retry), then re-arm here.
//               On a FAILED run this also revives the run (failed→running).
//   cancel_task pending | blocked → cancelled + task.cancelled event.
//               Running tasks are refused — that is the run-level stop.
//   Task-level actions on a cancelling/cancelled/succeeded run are refused
//   loud at the RUN level (dag.invalid_run_state): stop is the operator's
//   terminal and a finalized run holds no revivable node — plan a new run.

import { defineTool } from '@deepseek-ai/dsh-tools'

/** §8.4 action quick-reference, embedded in the description. */
const ACTION_SUMMARY = [
  'Actions: pause — stop admitting new work, let in-flight attempts finish, then the run drains to paused (call dag_tick to drive the drain);',
  'resume — reopen a paused run (paused→running);',
  'stop — cancel everything: non-running tasks cancel immediately, in-flight attempts abort and land cancelled on the next dag_tick, which also aggregates the run to cancelled (call dag_tick after stopping to finish the harvest; attempts that register after the stop are swept again on every later tick — the abort is idempotent);',
  'retry_task — re-arm a failed, blocked(upstream_failed), or blocked(merge_conflicted) task into retry_wait now (task_id required; for a conflicted merge node, resolve the worktrees queue first via worktree_queue resolve/retry — this is its only re-run edge; manual retries do NOT consume the spec retryOn budget; on a failed run this also revives the run to running — a cancelling/cancelled/succeeded run refuses the action instead);',
  'cancel_task — cancel a pending or blocked task (task_id required; a running task must be stopped at run level via stop).',
].join(' ')

/**
 * Register the dag_control tool. deps = { engine, store, config }. Returns
 * undefined.
 *
 * @param {{tools: {register: Function}}} ctx plugin context (fake in tests)
 * @param {{engine: object, store: object, config?: object}} deps
 */
export function registerDagControl(ctx, deps) {
  const { engine, store } = deps
  if (!engine || typeof engine.control !== 'function') {
    throw new Error('dag_control: requires deps.engine with control')
  }
  if (!store || typeof store.findRun !== 'function') {
    throw new Error('dag_control: requires deps.store with findRun')
  }

  ctx.tools.register(defineTool({
    name: 'dag_control',
    description: 'Control a DAG run or one of its tasks: pause / resume / stop at run level, retry_task / cancel_task at node level. '
      + ACTION_SUMMARY
      + ' Illegal source states fail loud (dag.invalid_run_state / dag.invalid_task_state); re-sending an action already in effect is an idempotent re-affirm.',
    parameters: {
      run_id: {
        type: 'string',
        required: true,
        description: 'The run to control.',
      },
      action: {
        type: 'string',
        required: true,
        enum: ['pause', 'resume', 'stop', 'retry_task', 'cancel_task'],
        description: 'The control action (see the semantics above).',
      },
      task_id: {
        type: 'string',
        description: 'retry_task/cancel_task target (required for those two actions).',
      },
      reason: {
        type: 'string',
        description: 'Recorded on the control event (recommended: why the action is taken).',
      },
    },
    output: {
      // §8.4 control summary — produced by the engine; the effected[] rows
      // are task-level only (run-level actions carry an empty array and
      // report through run_state). Open root keeps the engine as the single
      // owner of that contract.
      schema: { type: 'object', additionalProperties: true },
      render: (_args, value) => [{ type: 'text', text: JSON.stringify(value, null, 2) }],
    },
    isConcurrencySafe: () => false,
    async execute(args, exec) {
      if (store.findRun(args.run_id) === null) {
        const failure = new Error(`dag_control: dag.run_not_found — ${args.run_id}`)
        failure.code = 'dag.run_not_found'
        throw failure
      }
      // Conditional expansion — no undefined-valued keys (json-safe).
      const options = {}
      if (args.task_id !== undefined) options.taskId = args.task_id
      if (args.reason !== undefined) options.reason = args.reason
      options.execAgent = exec?.agent
      return engine.control(args.run_id, args.action, options)
    },
  }))
}
