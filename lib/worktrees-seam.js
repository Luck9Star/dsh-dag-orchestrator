// worktrees-seam — the T15 composition seam to the dsh-worktrees engine
// (DESIGN §11.2 seam 2: engine-singleton acquisition).
//
// WHY THIS MODULE EXISTS. The merge executor (T17) reuses dsh-worktrees'
// merge queue instead of building a second one (worktrees §10 forbids a
// second merge-queue instance over the same state.json), and worktree-
// declaring tasks (T16) reuse its worktree service. Both need the LIVE
// singletons created inside dsh-worktrees' apply() closure — reachable
// only through the Cordis service face.
//
// ACQUISITION STRATEGY — opportunistic probing, NOT inject.
//
//   The plugin does NOT declare `worktreesEngine` in its `inject` array.
//   Per DESIGN §11.2 (verbatim intent): "inject 会把本插件的加载阻塞到
//   依赖可用为止，可选组合缝不应有此权力" — inject would block this
//   plugin's load until the dependency is available, and an OPTIONAL
//   composition seam must not have that power. Agent-only DAGs must boot
//   with dsh-worktrees absent entirely. `ctx.get(name)` is the sanctioned
//   read-without-declare face (Cordis reflect contract: returns the
//   service value, or undefined when not (yet) provided — the same
//   pattern as the dsh-plugin-subagents native driver's `ctx.get('jobs')`
//   and bridge driver's `ctx.get('sessions')` probes).
//
// PROBE CADENCE — apply time + every use. `createWorktreesSeam` is called
// once in apply() (which immediately probes once, so the apply-time log
// line can state availability honestly), and `get()` RE-PROBES on every
// call: a late-loading engine (host loads dsh-worktrees after us) still
// gets picked up by the next consumer call. Presence is never cached as
// "absent forever".
//
// ZERO CODE COUPLING: this module never imports dsh-worktrees (red line —
// the service face is the only channel). The contract below is the
// consumer-side JSDoc anchor for the dsh-worktrees provider task.

/**
 * The service contract dsh-worktrees must provide (DESIGN §11.2's
 * recommended shape — provider side: `ctx.provide('worktreesEngine', …)`
 * inside its apply, over the SAME service/queue singletons its tools use):
 *
 * @typedef {object} WorktreesEngineFace
 * @property {() => object} getMergeQueue
 *           The live MergeQueue singleton (dsh-worktrees lib/merge-queue.js
 *           return face). DAG merge tasks (T17) consume:
 *             enqueue({worktreeId, integrationBranch, origin:'dag',
 *                      correlationId}) → job record `{repoKey, …}`
 *           (the FOUR-key DAG contract — git facts are NOT passed; the
 *           provider resolves repoKey/repoRoot/sourceBranch from the
 *           worktree record server-side, and the returned repoKey keys the
 *           drain), and
 *             drain(repoKey, integrationBranch) → Promise<DrainOutcome>
 *           (five-state outcome: succeeded | conflicted | failed |
 *           no_changes | queued_ahead — T17 maps them to task outcomes).
 *           The canonical consumer contract anchor is
 *           lib/executors/merge.js's module header; dsh-worktrees
 *           implements to that (see the provider-adapter TODO in
 *           CHANGELOG Known issue ①).
 * @property {() => object} getWorktreeService
 *           The live WorktreeService singleton (dsh-worktrees
 *           lib/worktree-service.js return face). DAG worktree tasks (T16)
 *           consume it through THREE contract methods:
 *
 *             create({task, repoRoot, baseRef, origin:'dag',
 *                     correlationId: attemptId}) → {id, path, branch, …}
 *           REQUIRED. The record's `path` becomes the task subagent's cwd
 *           (after the red-line-9 gate, baseCwd = projectRoot). The DAG
 *           never deletes worktrees — lifecycle stays with the worktrees
 *           plugin's own merge collection / reconcile (DESIGN §11.3).
 *           A create that THROWS (or returns a record without a usable
 *           `path`) maps to transient `dag.worktree_create_failed` —
 *           creation commonly fails on locks/staging races, and retrying
 *           is sound.
 *
 *           T17: the record's `id` field is additionally REQUIRED for
 *           merge-source wiring — the DAG stamps it into the upstream
 *           attempt's `attempt.dispatched` event payload, and the merge
 *           executor reads it back as the merge source (event-sourced,
 *           zero new tables). Degradation is honest and late: a record
 *           with a usable `path` but NO usable `id` still yields a usable
 *           cwd for the agent task itself; the failure surfaces only when
 *           a downstream merge task cannot resolve that source
 *           (`dag.merge_source_missing`), so older path-only services keep
 *           plain worktree tasks working.
 *
 *             findActiveByTask(repoRoot, taskSlug) → active record | null
 *           OPTIONAL (contract since T16). When present, the dispatch-time
 *           reuse probe consults it BEFORE creating: an active worktree
 *           record for the same (repoRoot, task slug) → its `path` is
 *           REUSED, no second worktree is created (DESIGN §11.3: "retry_task
 *           重派发时若 worktree 记录仍 active 则复用 path，不重复建").
 *           WHY THE KEY IS THE TASK SLUG, NOT correlationId: a retry is a
 *           NEW attemptId, hence a NEW correlationId — a correlation lookup
 *           could never find the prior attempt's worktree. The task slug is
 *           stable across attempts. A service without this method degrades
 *           honestly: a fresh worktree every attempt (create stays the
 *           source of truth; the DAG never fabricates records).
 *
 *           M3 review M-A — the returned record's `correlationId` field
 *           (OPTIONAL, string) is the OWNERSHIP evidence for reuse: the
 *           record is reused only when its correlationId (the create-time
 *           attemptId the worktree was correlated to) belongs to the
 *           dispatching task's own attempt history — §11.3 authorizes
 *           same-task re-dispatch reuse ONLY, never cross-task sharing
 *           (spec-validate additionally rejects duplicate worktree slugs
 *           across tasks, `dag.worktree_slug_conflict`). A record WITHOUT
 *           a usable correlationId is conservatively NOT reused (honest
 *           degradation: create fresh; a record that cannot prove
 *           ownership must not hand out its path). Provider contract:
 *           return the record's correlationId when the record has one —
 *           the reuse probe consumes exactly `{path, id?, correlationId?}`.
 *
 *             findActiveByCorrelation(correlationId) → active record | null
 *           OPTIONAL, reserved. Not consulted by T16's retry path for the
 *           reason above (the retry's correlationId differs from the
 *           original attempt's); recorded here so the provider implements
 *           ONE lookup family with consistent semantics (exact-record
 *           probes for diagnostics/reconciliation, e.g. crash-window
 *           orphan auditing per DESIGN §11.3's 对账 note).
 * @property {boolean} [available]
 *           DESIGN's suggested convenience flag. The seam does NOT trust it
 *           for admission — the two function members are the authoritative
 *           shape test (a stale `available: true` on a half-shaped face
 *           must not half-wire the seam; a missing flag on a fully-shaped
 *           face must not disable it).
 *
 * CONSUMER-SIDE DEGRADATION (T16/T17 territory, stated here for the
 * contract anchor): when `get().worktrees === null`, merge/worktree tasks
 * fail LOUD with permanent `dag.worktrees_unavailable` at dispatch — never
 * a silent skip and never a fallback second queue. Agent-only DAGs are
 * unaffected (no consumer of the seam is ever consulted for them).
 */

/**
 * Create the worktrees composition seam.
 *
 * @param {object} ctx Cordis ctx (or a fake). Only `ctx.get` is read, and
 *   ONLY as `typeof ctx.get === 'function'` — a ctx without the face
 *   (strict test doubles) simply reports unavailable.
 * @param {{warn?: Function, debug?: Function, info?: Function}} [logger]
 *   Host logger; only `warn` is used (probe-thrown and shape-reject
 *   diagnostics — absence must be visible but never fatal).
 * @returns {{
 *   probe: () => {available: boolean, engine?: object},
 *   get: () => {worktrees: {getMergeQueue: () => object, getWorktreeService: () => object} | null},
 * }} the seam handle. State carries exactly one field (the last healthy
 *   reference, kept for the `engine` probe report) — no timers, no
 *   subscriptions, no teardown.
 */
export function createWorktreesSeam(ctx, { logger } = {}) {
  const log = logger ?? {}
  /** Last accepted engine reference — kept for reporting, never for reuse:
   * every get() re-probes, so a disposed-then-reprovided engine cannot be
   * served stale from here. */
  let lastEngine = undefined

  /**
   * Shape admission (fail-safe, no half-accept): the face is usable iff it
   * is a non-null object exposing BOTH getMergeQueue and getWorktreeService
   * as functions. Anything else — undefined, null, a half shape — is
   * treated EXACTLY like absence (DESIGN §11.2: absence degrades loudly at
   * the consumer; a half-wired seam is strictly worse than none).
   *
   * @param {unknown} candidate whatever ctx.get('worktreesEngine') returned
   * @returns {object | null} the candidate when fully shaped, else null
   */
  function admit(candidate) {
    if (candidate === null || typeof candidate !== 'object') return null
    if (typeof candidate.getMergeQueue !== 'function') return null
    if (typeof candidate.getWorktreeService !== 'function') return null
    return candidate
  }

  /**
   * One opportunistic read of the service face. EVERY failure mode maps to
   * `{available: false}` — a throwing or weird ctx must never crash apply()
   * or a dispatch loop (the probe is wrapped: catch → warn → absent).
   *
   * @returns {{available: boolean, engine?: object}}
   */
  function probe() {
    let raw
    try {
      if (!ctx || typeof ctx.get !== 'function') return { available: false }
      raw = ctx.get('worktreesEngine')
    } catch (error) {
      log.warn?.(
        `dsh-dag-orchestrator: worktreesEngine probe threw (treated as absent): ${String(error?.message ?? error)}`,
      )
      return { available: false }
    }
    const engine = admit(raw)
    if (engine === null) {
      // Half-shaped faces get one warn per probe: silent shape rejection
      // would read as "dsh-worktrees not loaded" and cost an operator an
      // afternoon. Plain absence (undefined) stays silent — it is the
      // normal agent-only deployment, not a fault.
      if (raw !== undefined) {
        log.warn?.(
          'dsh-dag-orchestrator: ctx.get(\'worktreesEngine\') returned a value without the '
            + 'required getMergeQueue()/getWorktreeService() functions — treated as absent '
            + '(expected the dsh-worktrees engine face, see lib/worktrees-seam.js WorktreesEngineFace)',
        )
      }
      return { available: false }
    }
    lastEngine = engine
    return { available: true, engine }
  }

  return {
    probe,

    /**
     * The consumer entry (T16/T17 call this at use time — which is exactly
     * the "re-probe on every use" cadence). Re-probes FIRST: a reference
     * accepted by an earlier probe is never reused blindly, so a
     * late-provided engine is picked up and an unloaded one is not served
     * stale. Returns the narrow `{worktrees}` wrapper or `null` degraded.
     *
     * @returns {{worktrees: {getMergeQueue: () => object, getWorktreeService: () => object} | null}}
     */
    get() {
      const verdict = probe()
      if (!verdict.available) return { worktrees: null }
      return {
        worktrees: {
          // Identity pass-through by design: the SAME singletons the
          // worktrees tools use — this seam adds zero wrapping that could
          // drift from the provider's contract.
          getMergeQueue: () => verdict.engine.getMergeQueue(),
          getWorktreeService: () => verdict.engine.getWorktreeService(),
        },
      }
    },

    /** Last admitted engine reference (diagnostics only; may be undefined). */
    get engineRef() {
      return lastEngine
    },
  }
}
