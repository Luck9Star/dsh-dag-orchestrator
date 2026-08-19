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
 *   runsForSession(sessionId)    all-runs summary rows filtered to
 *                                runs.planner_session = sessionId.
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
 * Aggregate task-state counts for one run — a verbatim mirror of
 * engine.status's countsOf (§8.2 summary rows): queued folds into ready,
 * retry_wait into pending, cancelled into failed. Kept here so the
 * session-filtered rows are shape-identical to the all-runs rows.
 *
 * @param {Array<object>} tasks
 */
function countsOfTasks(tasks) {
  const counts = { pending: 0, ready: 0, running: 0, succeeded: 0, failed: 0, blocked: 0 }
  for (const task of tasks) {
    if (Object.hasOwn(counts, task.state)) counts[task.state]++
    else if (task.state === 'queued') counts.ready++
    else if (task.state === 'retry_wait') counts.pending++
    else if (task.state === 'cancelled') counts.failed++
  }
  return counts
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
 *   runsForSession: Function,
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
   * Fallback run source for stores predating findRunsByPlannerSession
   * (mirrors engine.allRunIds's fallback discipline): findAllRuns when
   * present, else the non-terminal scan (older/fake stores).
   *
   * @returns {Array<object>}
   */
  function findAllRunsCompat() {
    if (typeof store.findAllRuns === 'function') return store.findAllRuns()
    return store.findNonTerminalRuns()
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

  /**
   * All-runs summary rows filtered to the planning session — the GUI tab's
   * "this conversation's runs" section. Row shape matches the
   * status(undefined, {detail:'summary'}) arm exactly (run_id, name, state,
   * counts, created_at, updated_at) so the client renders both sections
   * with one component. An unknown session yields `{runs: []}` — an empty
   * result is a state, not an error. Legacy rows (NULL planner_session,
   * pre-column databases) never match.
   *
   * @param {string} sessionId
   * @returns {{runs: Array<object>}}
   */
  function runsForSession(sessionId) {
    if (typeof sessionId !== 'string' || sessionId.length === 0) {
      throw faceError('dag.run_not_found', `session ${JSON.stringify(sessionId)} has no runs`)
    }
    const source = typeof store.findRunsByPlannerSession === 'function'
      ? store.findRunsByPlannerSession(sessionId)
      : findAllRunsCompat().filter((run) => run.planner_session === sessionId)
    return {
      runs: source.map((run) => ({
        run_id: run.run_id,
        name: run.name,
        state: run.state,
        counts: countsOfTasks(store.findTasks(run.run_id)),
        created_at: run.created_at,
        updated_at: run.updated_at,
      })),
    }
  }

  return Object.freeze({
    status,
    getSpec,
    listOutputs,
    attemptSummaries,
    runsForSession,
  })
}
