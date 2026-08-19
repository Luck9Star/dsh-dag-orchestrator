/**
 * dag-face — read-only service face for plugin-to-plugin consumers
 * (`ctx.provide('dagOrchestrator', face)` from apply()).
 *
 * Glue only: no state of its own, no sqlite, no host-service imports.
 * The UI host half (and any later consumer) reads through this face over
 * the SAME engine/store singletons the dag_* tools use. Control stays on
 * the tools; this module never writes a projection table.
 *
 * Surface:
 *   status(runId?, options?)     engine.status passthrough (four depths).
 *   getSpec(runId)               parsed runs.spec_json + spec_hash.
 *   listOutputs(runId)           outputs table, value_json parsed.
 *   attemptSummaries(runId, taskId?)
 *                                attempts rows + parsed result_json as
 *                                `summary` when present.
 *
 * Unknown run → Error whose message starts `dag.run_not_found` (and
 * `.code === 'dag.run_not_found'`). Conditional expansion: never emit
 * undefined-valued keys. The returned face is frozen.
 */

/**
 * Loud face error with a stable `dag.*` code. The message starts with the
 * code so consumers can match either `.code` or `.message`.
 *
 * @param {string} code
 * @param {string} detail
 * @returns {Error}
 */
function faceError(code, detail) {
  const error = new Error(`${code}: ${detail}`)
  error.code = code
  return error
}

/**
 * @param {object} store
 * @param {unknown} runId
 * @returns {object} the run row
 */
function requireExistingRun(store, runId) {
  if (typeof runId !== 'string' || runId.length === 0) {
    throw faceError('dag.run_not_found', `run ${JSON.stringify(runId)} not found`)
  }
  const run = store.findRun(runId)
  if (run === null || run === undefined) {
    throw faceError('dag.run_not_found', `run ${JSON.stringify(runId)} not found`)
  }
  return run
}

/**
 * Build the frozen read-only `dagOrchestrator` face.
 *
 * @param {{engine: object, store: object, logger?: object}} deps
 * @returns {{
 *   status: Function,
 *   getSpec: Function,
 *   listOutputs: Function,
 *   attemptSummaries: Function,
 * }}
 */
export function createDagFace(deps = {}) {
  const { engine, store } = deps
  if (!engine || typeof engine.status !== 'function') {
    throw new Error('dag-face: requires deps.engine with status')
  }
  if (
    !store
    || typeof store.findRun !== 'function'
    || typeof store.findTasks !== 'function'
    || typeof store.findAttempts !== 'function'
    || typeof store.findOutputsByTask !== 'function'
  ) {
    throw new Error('dag-face: requires deps.store with findRun/findTasks/findAttempts/findOutputsByTask')
  }

  /**
   * engine.status passthrough. undefined / null / '' all mean all-runs,
   * matching the tool layer's empty-run_id normalisation (engine.status
   * already treats the same three as the summary arm).
   *
   * @param {string | null | undefined} [runId]
   * @param {object} [options]
   */
  function status(runId, options) {
    const id = runId === undefined || runId === null || runId === '' ? undefined : runId
    return engine.status(id, options)
  }

  /**
   * @param {string} runId
   * @returns {{run_id: string, name: string, spec_hash: string, spec: object}}
   */
  function getSpec(runId) {
    const run = requireExistingRun(store, runId)
    return {
      run_id: run.run_id,
      name: run.name,
      spec_hash: run.spec_hash,
      spec: JSON.parse(run.spec_json),
    }
  }

  /**
   * Compose a run's outputs from per-task lookups (store has
   * findOutputsByTask / findAllOutputs, not a per-run reader).
   *
   * @param {string} runId
   * @returns {Array<{task_id: string, name: string, value: unknown, produced_by_attempt: string}>}
   */
  function listOutputs(runId) {
    requireExistingRun(store, runId)
    const rows = []
    for (const task of store.findTasks(runId)) {
      for (const output of store.findOutputsByTask(runId, task.task_id)) {
        rows.push({
          task_id: output.task_id,
          name: output.name,
          value: JSON.parse(output.value_json),
          produced_by_attempt: output.produced_by_attempt,
        })
      }
    }
    return rows
  }

  /**
   * @param {string} runId
   * @param {string} [taskId]
   * @returns {Array<object>}
   */
  function attemptSummaries(runId, taskId) {
    requireExistingRun(store, runId)
    const taskIds = taskId !== undefined
      ? [taskId]
      : store.findTasks(runId).map((task) => task.task_id)
    const rows = []
    for (const id of taskIds) {
      for (const attempt of store.findAttempts(runId, id)) {
        const entry = {
          attempt_id: attempt.attempt_id,
          task_id: attempt.task_id,
          ordinal: attempt.ordinal,
          state: attempt.state,
          backend: attempt.backend,
          started_at: attempt.started_at,
        }
        if (attempt.child_session !== null && attempt.child_session !== undefined) {
          entry.child_session = attempt.child_session
        }
        if (attempt.stop_reason !== null && attempt.stop_reason !== undefined) {
          entry.stop_reason = attempt.stop_reason
        }
        if (attempt.result_json !== null && attempt.result_json !== undefined && attempt.result_json !== '') {
          entry.summary = JSON.parse(attempt.result_json)
        }
        rows.push(entry)
      }
    }
    return rows
  }

  return Object.freeze({
    status,
    getSpec,
    listOutputs,
    attemptSummaries,
  })
}
