/**
 * cwd-gate — red line 9: the dispatch-time cwd gate (DESIGN §4.6/§10-R9).
 *
 * One pure-ish decision function, `resolveTaskCwd({taskCwd, baseCwd,
 * allowedRoots, requireWorkspaceRegistration})`, consumed by the executor
 * BEFORE any request carries a cwd (TASKS.md T06 dispatch layer; the plan
 * layer deliberately only picks the base — lib/tools/dag-plan.js).
 *
 * Semantics (DESIGN §4.6 "M1" row):
 *   * `taskCwd` ABSENT  → the run's `baseCwd` (no decision to make; the
 *     base is the session cwd dag_plan resolved — trusted by construction).
 *   * `taskCwd` PRESENT → must be an ABSOLUTE path, must EXIST (realpath),
 *     and its realpath must fall inside the baseCwd subtree OR inside ANY
 *     config.allowedRoots subtree (worktrees repo-gate's judgment shape,
 *     locally implemented — independent packages, no import).
 *   * Symlink hardening: containment is judged on REALPATHS of both ends
 *     (`/tmp/repo/link → /etc` does not smuggle /etc past the gate) and on
 *     the task side of the baseCwd too (a symlinked base cannot expand the
 *     boundary; it only keeps its real location).
 *   * `requireWorkspaceRegistration: true` → DENY every explicit taskCwd.
 *     M1 has NO workspaceRegistry probing channel (DESIGN §4.6 lists the
 *     registry as part of the union; the M1 probe is absent —
 *     lib/tools/dag-plan.js resolveBaseCwd documents the same narrowing).
 *     A switch that demands a registry the host cannot consult must fail
 *     CLOSED: allowing would reduce the flag to a no-op.
 *
 * Discipline: no returns with undefined-valued keys; denial reasons carry a
 * stable `dag.cwd_denied` code (the caller maps the denial to a permanent
 * dispatch failure).
 */

import { realpathSync } from 'node:fs'
import { isAbsolute, sep } from 'node:path'

/**
 * Realpath one path; `null` when it does not exist (the gate's own
 * "nonexistent → deny" arm — never an exception).
 *
 * @param {string} p
 * @returns {string | null}
 */
function realOrNull(p) {
  try {
    return realpathSync(p)
  } catch {
    return null
  }
}

/**
 * Is `child` (an absolute, already-realpath'd path) inside the subtree of
 * `root` (also realpath'd)? Equal paths count as inside (the root itself).
 *
 * @param {string} child
 * @param {string} root
 * @returns {boolean}
 */
function isInside(child, root) {
  if (child === root) return true
  return child.startsWith(root.endsWith(sep) ? root : root + sep)
}

/**
 * Resolve the effective cwd for one task dispatch through the red-line-9
 * gate. PURE DECISION FUNCTION: no store, no engine, no subagents.
 *
 * @param {object} input
 * @param {string | undefined} [input.taskCwd] spec task.cwd (optional; spec
 *        validation already rejects relative values — the gate re-checks,
 *        fail-closed, because it is the security boundary).
 * @param {string} [input.baseCwd] the run's base cwd (run.base_cwd).
 * @param {string[]} [input.allowedRoots] config.allowedRoots (default []).
 * @param {boolean} [input.requireWorkspaceRegistration] config switch
 *        (default false). M1: true → explicit taskCwd is denied (fail-closed,
 *        see module header).
 * @returns {{ok: true, cwd: string}
 *   | {ok: false, code: 'dag.cwd_denied', message: string}}
 */
export function resolveTaskCwd({ taskCwd, baseCwd, allowedRoots, requireWorkspaceRegistration } = {}) {
  const roots = Array.isArray(allowedRoots) ? allowedRoots : []

  // Absent taskCwd → the run's base (default = the run base; a missing
  // baseCwd with an absent taskCwd is a programming error and fails loud).
  if (taskCwd === undefined || taskCwd === null) {
    if (typeof baseCwd !== 'string' || baseCwd.length === 0) {
      return {
        ok: false,
        code: 'dag.cwd_denied',
        message: 'no task.cwd declared and the run carries no base_cwd — nowhere to dispatch',
      }
    }
    return { ok: true, cwd: baseCwd }
  }

  if (typeof taskCwd !== 'string' || !isAbsolute(taskCwd)) {
    return {
      ok: false,
      code: 'dag.cwd_denied',
      message: `task.cwd ${JSON.stringify(taskCwd)} must be an absolute path`,
    }
  }

  // Fail-closed registry demand (M1: no probing channel exists — see header).
  if (requireWorkspaceRegistration === true) {
    return {
      ok: false,
      code: 'dag.cwd_denied',
      message:
        'task.cwd requires workspace registration (config.requireWorkspaceRegistration=true) '
        + 'but no workspace registry channel exists in M1 — refusing rather than bypassing the gate',
    }
  }

  if (typeof baseCwd !== 'string' || baseCwd.length === 0) {
    return {
      ok: false,
      code: 'dag.cwd_denied',
      message: `task.cwd ${JSON.stringify(taskCwd)} cannot be gated without the run's base cwd`,
    }
  }

  const realTask = realOrNull(taskCwd)
  if (realTask === null) {
    return {
      ok: false,
      code: 'dag.cwd_denied',
      message: `task.cwd ${JSON.stringify(taskCwd)} does not exist (realpath failed)`,
    }
  }
  const realBase = realOrNull(baseCwd)
  if (realBase === null) {
    return {
      ok: false,
      code: 'dag.cwd_denied',
      message: `run base cwd ${JSON.stringify(baseCwd)} does not exist (realpath failed)`,
    }
  }

  if (isInside(realTask, realBase)) return { ok: true, cwd: taskCwd }
  for (const root of roots) {
    if (typeof root !== 'string' || root.length === 0) continue
    const realRoot = realOrNull(root)
    if (realRoot !== null && isInside(realTask, realRoot)) return { ok: true, cwd: taskCwd }
  }

  return {
    ok: false,
    code: 'dag.cwd_denied',
    message:
      `task.cwd ${JSON.stringify(taskCwd)} (realpath ${JSON.stringify(realTask)}) is outside `
      + `the run base ${JSON.stringify(realBase)} and outside every allowed root `
      + `(${roots.map((r) => JSON.stringify(r)).join(', ') || 'none configured'})`,
  }
}
