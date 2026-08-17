/**
 * BoundedQueue — in-memory ready queue with capacity enforcement.
 *
 * Ported from task-weaver packages/scheduler/src/bounded-queue.ts (L1-150,
 * "B3.6") — type erasure only; zero logic changes, the 5-level comparator is
 * verbatim (DESIGN §9.1).
 *
 * Source rationale (workflow-engine.md "资源准入"): "first enter the bounded
 * ready queue, then request resources. A task that cannot obtain resources
 * stays queued and does not hold partial resources, forming a deadlock." This
 * queue is the bounded hop BEFORE resource admission: the scheduler enqueues
 * ready tasks up to `queueCapacity`, then acquires concurrency slots for the
 * admitted subset. Overflow is NOT an error — the caller keeps the task in
 * `ready` and re-evaluates next tick (so the queue never becomes a deadlock
 * source).
 *
 * Deterministic priority (workflow-engine.md "公平性与优先级"):
 *   1. human-unblocked (true before false);
 *   2. critical-path-depth (descending);
 *   3. explicit priority (descending);
 *   4. ready-time (ascending — earlier first);
 *   5. task_id lexicographic (ascending stable tiebreak).
 *
 * The queue is in-memory (single-process scheduler). `Limits.queueCapacity`
 * (the validated spec) is the source of the capacity value; the caller passes
 * it in. A projection-backed variant is not needed because the durable
 * TaskGraph is the source of truth and this queue is rebuilt each tick from
 * the ready set — overflow simply defers to the next reconcile.
 */

/**
 * One entry in the bounded ready queue. The fields mirror the deterministic
 * priority inputs from workflow-engine.md "公平性与优先级".
 *
 * @typedef {object} QueueEntry
 * @property {string} taskId Stable spec node id (stable within a run).
 * @property {boolean} humanUnblocked True if this task was unblocked by a human/external action (ranked first).
 * @property {number} criticalPathDepth Depth on the critical path (longest chain to a leaf); higher ranks earlier.
 * @property {number} priority Spec explicit priority; higher ranks earlier.
 * @property {number} readyAt Epoch-ms when the task became ready; earlier ranks earlier (FIFO tiebreak).
 */

/**
 * Deterministic comparator implementing the 5-level priority from
 * workflow-engine.md "公平性与优先级". Returns negative if `a` ranks before `b`.
 *
 * Exported for unit testing of the ordering contract in isolation.
 *
 * @param {QueueEntry} a
 * @param {QueueEntry} b
 * @returns {number}
 */
export function compareQueueEntries(a, b) {
  // 1. human-unblocked first (true < false → treat true as higher rank).
  if (a.humanUnblocked !== b.humanUnblocked) {
    return a.humanUnblocked ? -1 : 1;
  }
  // 2. critical-path-depth descending.
  if (a.criticalPathDepth !== b.criticalPathDepth) {
    return b.criticalPathDepth - a.criticalPathDepth;
  }
  // 3. explicit priority descending.
  if (a.priority !== b.priority) {
    return b.priority - a.priority;
  }
  // 4. ready-time ascending (earlier first).
  if (a.readyAt !== b.readyAt) {
    return a.readyAt - b.readyAt;
  }
  // 5. task_id lexicographic ascending (stable tiebreak).
  if (a.taskId < b.taskId) return -1;
  if (a.taskId > b.taskId) return 1;
  return 0;
}

/**
 * Bounded ready queue. Not a ring/linked list — the queue is small (≤1024 per
 * the Limits policy) and rebuilt each tick, so an array re-sorted on drain is
 * both simple and fast enough. Capacity is enforced on tryEnqueue().
 */
export class BoundedQueue {
  #entries = [];
  #cap;

  /**
   * @param {number} queueCapacity
   */
  constructor(queueCapacity) {
    if (!Number.isInteger(queueCapacity) || queueCapacity < 1) {
      throw new Error(
        `BoundedQueue: queueCapacity must be a positive integer (got ${queueCapacity})`,
      );
    }
    this.#cap = queueCapacity;
  }

  /** The configured capacity (mirrors Limits.queueCapacity). */
  get capacity() {
    return this.#cap;
  }

  /** Current number of enqueued entries. */
  get size() {
    return this.#entries.length;
  }

  /** True when no entries are enqueued. */
  get isEmpty() {
    return this.#entries.length === 0;
  }

  /**
   * Try to enqueue an entry. Returns `true` if admitted, `false` if the queue
   * is at capacity (the caller keeps the task in `ready` and re-evaluates next
   * tick — NEVER throws, per workflow-engine.md "资源准入").
   *
   * Duplicate taskIds are allowed to coexist (the caller dedupes against the
   * TaskGraph); this queue is a dumb ordering structure.
   *
   * @param {QueueEntry} entry
   * @returns {boolean}
   */
  tryEnqueue(entry) {
    if (this.#entries.length >= this.#cap) return false;
    this.#entries.push(entry);
    return true;
  }

  /**
   * Return the highest-priority entry without removing it, or `undefined` if
   * empty. Computed by a stable sort over the current entries.
   *
   * @returns {QueueEntry | undefined}
   */
  peek() {
    if (this.#entries.length === 0) return undefined;
    // Copy + sort to avoid mutating insertion order until drain/remove.
    return [...this.#entries].sort(compareQueueEntries)[0];
  }

  /**
   * Remove and return ALL entries in deterministic priority order, leaving the
   * queue empty. The scheduler calls this once per tick after resource
   * admission so the queue is rebuilt fresh from the ready set next reconcile.
   *
   * @returns {QueueEntry[]}
   */
  drain() {
    const sorted = this.#entries.splice(0, this.#entries.length).sort(compareQueueEntries);
    return sorted;
  }

  /**
   * Remove a specific entry by taskId. Returns `true` if it was present and
   * removed, `false` otherwise. Used when a task is cancelled or no longer
   * eligible while waiting in the queue.
   *
   * @param {string} taskId
   * @returns {boolean}
   */
  remove(taskId) {
    const idx = this.#entries.findIndex((e) => e.taskId === taskId);
    if (idx === -1) return false;
    this.#entries.splice(idx, 1);
    return true;
  }
}
