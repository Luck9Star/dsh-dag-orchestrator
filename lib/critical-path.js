/**
 * Critical-path depth calculator.
 *
 * Ported from task-weaver packages/scheduler/src/critical-path.ts (L1-33,
 * "extracted from scheduler-loop.ts, H-WP22") — type-erasure only; the
 * downstream-map + memoized DFS logic is unchanged.
 *
 * Pure function: given a task id and a DAG spec, returns the maximum chain
 * length from that task to any leaf downstream. Used by the queue builder to
 * prioritize tasks on longer critical paths.
 *
 * Spec shape (this plugin, pre- and post-zod — see DESIGN §7.2):
 *   { tasks: [{ id: string, dependsOn: [{ taskId: string, condition: string }] }] }
 * `dependsOn[].taskId` has the same field name as the source Dependency, so
 * only the type import is erased. `dependsOn` may be absent/undefined for
 * tasks without dependencies.
 *
 * PRECONDITION: the spec is acyclic (spec-validate rejects cycles before any
 * caller reaches this module — `dag.cycle_detected`). The source's DFS has no
 * visited-in-progress (gray-node) detection and relies on the same
 * precondition; ported as-is, per DESIGN §9.1.
 *
 * @param {string} taskId
 * @param {{ tasks: Array<{ id: string, dependsOn?: Array<{ taskId: string }> }> }} spec
 * @returns {number}
 */
export function criticalPathDepth(taskId, spec) {
  const downstreams = new Map();
  for (const t of spec.tasks) {
    for (const d of t.dependsOn ?? []) {
      const arr = downstreams.get(d.taskId) ?? [];
      arr.push(t.id);
      downstreams.set(d.taskId, arr);
    }
  }
  const memo = new Map();
  const dfs = (id) => {
    const cached = memo.get(id);
    if (cached !== undefined) return cached;
    const ds = downstreams.get(id) ?? [];
    if (ds.length === 0) {
      memo.set(id, 0);
      return 0;
    }
    const depth = 1 + Math.max(...ds.map(dfs));
    memo.set(id, depth);
    return depth;
  };
  return dfs(taskId);
}
