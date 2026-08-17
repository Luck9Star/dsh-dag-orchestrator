/**
 * Admission — in-memory resource admission for the dispatch loop
 * (DESIGN §5.4: C7 concurrency self-built, D11 no persistent leases).
 *
 * Two independent gates, both memory-only by design:
 *
 *   1. Agent slots — a PER-RUN counting semaphore (M6 fix: the pool used to
 *      be one global Set, so run A holding its full maxRunningAgents
 *      starved every other run in the process; §5.4's "上限 =
 *      run.limits.maxRunningAgents" is a PER-RUN cap). task-weaver's
 *      admission tries up to N times to grab any free process slot
 *      (process-slots.ts L53-71); the single-process narrowing is
 *      `bucket(runId).size < maxRunning`, with the bucket key riding the
 *      slot handle so release is owner-scoped. The bound stays DYNAMIC PER
 *      CALL (`tryAcquireSlot(maxRunning, runId)`), because one admission
 *      instance governs every run in the process and each run carries its
 *      own `limits.maxRunningAgents ?? config.defaultMaxRunningAgents`. A
 *      task that cannot get a slot stays `ready` and is re-evaluated next
 *      tick — admission failure is never an error and never holds partial
 *      resources (workflow-engine.md "资源准入": no deadlock from
 *      half-acquired resources).
 *
 *   2. Session keys — `shared_key` mutual exclusion (spec field
 *      `concurrencyKey`, DESIGN §7.1): at most ONE in-flight attempt per key
 *      process-wide (task-weaver acquireSessionLeases L1158-1186 plus the
 *      per-tick launchedSessionKeys serialization). The Map value is the
 *      holding attemptId; release is owner-checked so a stale or mis-routed
 *      caller cannot free another attempt's lease (防误释).
 *
 * Why nothing is persisted: this plugin's crash semantics are "host exits ⇒
 * every in-flight attempt is orphaned on recovery" (DESIGN §12) — a durable
 * lease would have no consumer after recovery. Memory semaphore + single
 * process = the same anti-concurrency effect with one less table and two
 * fewer failure modes. Honest cost (declared in README, open issue O5):
 * multiple dsh instances writing the same database are unprotected.
 *
 * Discipline (mirrors dag-store): lost races return false/null and NEVER
 * throw; malformed arguments are programming errors and throw loud
 * TypeErrors.
 */

/**
 * @typedef {object} SlotHandle
 * @property {number} slot Acquired slot index (lowest free index of the
 *   RUN'S OWN bucket below the caller's maxRunning at acquire time — index
 *   spaces are per-run, so run A's slot 0 and run B's slot 0 coexist).
 *   Opaque outside releaseSlot().
 * @property {string} runId The owning run's bucket key (M6) — releaseSlot
 *   reads it so a handle can only ever free its own run's pool.
 */

/**
 * Create an admission controller. The factory takes no configuration: both
 * bounds are supplied by the caller at use time (slots) or not at all
 * (session keys are unbounded by count — the spec caps how many keys exist).
 *
 * @returns {ReturnType<typeof createAdmission>}
 */
export function createAdmission() {
  /** @type {Map<string, Set<number>>} runId → that run's held slot indices (M6). */
  const slotsByRun = new Map();
  /** @type {Map<string, string>} sessionKey → holding attemptId */
  const sessionKeys = new Map();

  function requireNonEmptyString(value, label) {
    if (typeof value !== 'string' || value.length === 0) {
      throw new TypeError(`${label} must be a non-empty string (got ${typeof value})`);
    }
  }

  return {
    /**
     * Try to take one agent slot from runId's OWN pool under the caller's
     * dynamic bound. Returns a slot handle when `bucket(runId).size <
     * maxRunning`, null when that run's pool is full (the caller keeps the
     * task in `ready` — next tick re-evaluates; NEVER an error). Other
     * runs' pools are invisible to this check (M6: no cross-run starving).
     *
     * @param {number} maxRunning This run's concurrent-agent bound (engine
     *   passes `run.limits.maxRunningAgents ?? config.defaultMaxRunningAgents`;
     *   range validation 1..32 belongs to spec-validate, not here).
     * @param {string} runId The run whose pool is charged (M6).
     * @returns {SlotHandle | null}
     */
    tryAcquireSlot(maxRunning, runId) {
      if (!Number.isSafeInteger(maxRunning) || maxRunning < 1) {
        throw new TypeError(
          `tryAcquireSlot: maxRunning must be a positive safe integer (got ${JSON.stringify(maxRunning)})`,
        );
      }
      requireNonEmptyString(runId, 'tryAcquireSlot: runId');
      let held = slotsByRun.get(runId);
      if (held === undefined) {
        held = new Set();
        slotsByRun.set(runId, held);
      }
      if (held.size >= maxRunning) return null;
      // "N tries, any free slot" narrowed: with held < max some index in
      // [0, maxRunning) is free; take the lowest for determinism.
      for (let n = 0; n < maxRunning; n += 1) {
        if (!held.has(n)) {
          held.add(n);
          return { slot: n, runId };
        }
      }
      return null; // unreachable given the size guard above
    },

    /**
     * Release a slot handle. Returns true when a held slot was released,
     * false when it was not held (idempotent no-op for lost/duplicate
     * releases — never throws for a lost race). A HANDLE release is scoped
     * to its own run's bucket (M6 防误释: run B's handle can never free run
     * A's slot 0); a bare slot NUMBER (legacy/test shape, no bucket
     * information) scans the buckets and frees the first hit.
     *
     * @param {SlotHandle | number} handle
     * @returns {boolean}
     */
    releaseSlot(handle) {
      const n = typeof handle === 'number' ? handle : handle?.slot;
      if (!Number.isSafeInteger(n) || n < 0) {
        throw new TypeError(`releaseSlot: expected a slot handle or slot number (got ${JSON.stringify(handle)})`);
      }
      if (handle !== null && typeof handle === 'object' && typeof handle.runId === 'string') {
        const held = slotsByRun.get(handle.runId);
        if (held === undefined) return false;
        const released = held.delete(n);
        if (released && held.size === 0) slotsByRun.delete(handle.runId); // drop empty buckets
        return released;
      }
      for (const [runId, held] of slotsByRun) {
        if (held.delete(n)) {
          if (held.size === 0) slotsByRun.delete(runId); // drop empty buckets
          return true;
        }
      }
      return false;
    },

    /**
     * Try to become the single holder of a shared session key. Returns false
     * while ANY attempt holds it (strict mutex — even the current holder
     * re-acquiring returns false; the dispatch loop treats false as "leave
     * ready", fail-closed).
     *
     * @param {string} key The spec `concurrencyKey`.
     * @param {string} attemptId The claiming attempt.
     * @returns {boolean}
     */
    tryAcquireSessionKey(key, attemptId) {
      requireNonEmptyString(key, 'tryAcquireSessionKey: key');
      requireNonEmptyString(attemptId, 'tryAcquireSessionKey: attemptId');
      if (sessionKeys.has(key)) return false;
      sessionKeys.set(key, attemptId);
      return true;
    },

    /**
     * Release a session key, but only when the caller is the recorded holder
     * (防误释: a stale or mis-routed release must not free another attempt's
     * lease). Returns true when released.
     *
     * @param {string} key
     * @param {string} attemptId
     * @returns {boolean}
     */
    releaseSessionKey(key, attemptId) {
      requireNonEmptyString(key, 'releaseSessionKey: key');
      requireNonEmptyString(attemptId, 'releaseSessionKey: attemptId');
      if (sessionKeys.get(key) !== attemptId) return false;
      return sessionKeys.delete(key);
    },

    /** Total held agent slots across every run's pool. @returns {number} */
    heldCount() {
      let total = 0;
      for (const held of slotsByRun.values()) total += held.size;
      return total;
    },

    /**
     * This run's held slot count (M6 observability; 0 for an unknown run).
     * @param {string} runId @returns {number}
     */
    heldCountForRun(runId) {
      requireNonEmptyString(runId, 'heldCountForRun: runId');
      return slotsByRun.get(runId)?.size ?? 0;
    },

    /** True while some attempt holds the key. @param {string} key @returns {boolean} */
    isSessionKeyHeld(key) {
      requireNonEmptyString(key, 'isSessionKeyHeld: key');
      return sessionKeys.has(key);
    },
  };
}
