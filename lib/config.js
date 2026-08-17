/**
 * Configuration validation (T09, DESIGN §3 / TASKS.md T09).
 *
 * zod strict schema — invalid config, INCLUDING unknown/misspelled keys,
 * fails LOUDLY at apply time with the `dag.config_invalid` code instead of
 * surfacing as a confusing runtime error later (style follows
 * dsh-worktrees / dsh-plugin-subagents lib/config.js).
 *
 * Unlike those siblings (which throw), validateConfig returns the explicit
 * two-branch shape the apply() assembly consumes:
 *
 *   validateConfig(config) → {ok: true, value}   (fully resolved defaults)
 *                          | {ok: false, error}  (error.code = 'dag.config_invalid')
 *
 * The M2 register switches (control / approve) are part of the FINAL config
 * shape now: control's tool (dag_control, T11) is registered; approve's
 * (dag_approve, T12) is pending — a profile that carries it validates today
 * and apply() warns, never crashes (config shape is frozen at M1; only the
 * tool module is missing).
 */

import { z } from 'zod'
import os from 'node:os'
import path from 'node:path'

/** Default database file: `~/.dsh/dag-orchestrator/dag.db` (DESIGN §6.1). */
export function defaultDbPath() {
  return path.join(os.homedir(), '.dsh', 'dag-orchestrator', 'dag.db')
}

/**
 * Expand a leading `~` / `~/` against os.homedir (the default is expressed
 * with `~`; a user-supplied `~/...` dbPath must resolve the same way or the
 * store would create a literal '~' directory). Non-matching strings pass
 * through untouched; ':memory:' and absolute paths never start with '~'.
 */
export function expandHome(value) {
  if (typeof value !== 'string') return value
  if (value === '~') return os.homedir()
  if (value.startsWith('~/')) return path.join(os.homedir(), value.slice(2))
  return value
}

/**
 * Tool-family switches — all optional booleans, strict (an unknown switch
 * name is a config typo and fails loud). `control` is the T11 tool
 * (dag_control, registered); `approve` is T12's (dag_approve — its switch
 * pre-declares intent; apply() warns-not-crashes while the tool is
 * pending).
 */
export const registerSchema = z.object({
  plan: z.boolean().optional(),
  status: z.boolean().optional(),
  tick: z.boolean().optional(),
  control: z.boolean().optional(),
  approve: z.boolean().optional(),
}).strict()

/**
 * The full plugin config table. `.strict()` on every object level so
 * unknown keys (typos like `dbpath` or `maxRunningAgents`) reject at apply
 * time instead of being silently ignored.
 */
export const pluginConfigSchema = z.object({
  register: registerSchema.optional(),
  dbPath: z.string().min(1).optional(),
  defaultMaxRunningAgents: z.number().int().min(1).max(32).optional(),
  defaultQueueCapacity: z.number().int().min(1).max(1024).optional(),
  inputInlineLimitBytes: z.number().int().positive().optional(),
  // autoTickMs: the optional Timer interval for the no-dispatch reconcile
  // (T14, DESIGN §5.5). Default 0 = OFF — O3's lean is "M1 off / M2 default
  // 30s", but TASKS.md T14 keeps the conservative opt-in (an always-on
  // background state churn is a mental burden the operator should choose;
  // flipping the default later is a one-line change, no config migration).
  autoTickMs: z.number().int().min(0).optional(),
  allowedRoots: z.array(z.string().min(1)).optional(),
  requireWorkspaceRegistration: z.boolean().optional(),
}).strict()

/** register defaults: the whole tool family is on unless switched off. */
export const REGISTER_DEFAULTS = Object.freeze({
  plan: true,
  status: true,
  tick: true,
  control: true,
  approve: true,
})

/** Scalar defaults (TASKS.md T09 config table). */
export const SCALAR_DEFAULTS = Object.freeze({
  defaultMaxRunningAgents: 4,
  defaultQueueCapacity: 16,
  inputInlineLimitBytes: 32_768,
  autoTickMs: 0,
  requireWorkspaceRegistration: false,
})

/**
 * Validate + resolve the raw plugin config.
 *
 * @param {object} [config] raw host config
 * @returns {{ok: true, value: {
 *   register: {plan: boolean, status: boolean, tick: boolean, control: boolean, approve: boolean},
 *   dbPath: string, defaultMaxRunningAgents: number, defaultQueueCapacity: number,
 *   inputInlineLimitBytes: number, autoTickMs: number,
 *   allowedRoots: string[], requireWorkspaceRegistration: boolean,
 * }}}
 *   the final config with every default applied
 * @returns {{ok: false, error: Error}} error.code === 'dag.config_invalid';
 *   error.message lists every zod issue as `<path>: <message>` lines
 */
export function validateConfig(config = {}) {
  const result = pluginConfigSchema.safeParse(config)
  if (!result.success) {
    const detail = result.error.issues
      .map((issue) => `${issue.path.join('.') || '(root)'}: ${issue.message}`)
      .join('; ')
    const error = new Error(detail)
    error.code = 'dag.config_invalid'
    return { ok: false, error }
  }
  const raw = result.data
  return {
    ok: true,
    value: {
      register: { ...REGISTER_DEFAULTS, ...(raw.register ?? {}) },
      dbPath: expandHome(raw.dbPath ?? defaultDbPath()),
      defaultMaxRunningAgents: raw.defaultMaxRunningAgents ?? SCALAR_DEFAULTS.defaultMaxRunningAgents,
      defaultQueueCapacity: raw.defaultQueueCapacity ?? SCALAR_DEFAULTS.defaultQueueCapacity,
      inputInlineLimitBytes: raw.inputInlineLimitBytes ?? SCALAR_DEFAULTS.inputInlineLimitBytes,
      autoTickMs: raw.autoTickMs ?? SCALAR_DEFAULTS.autoTickMs,
      allowedRoots: raw.allowedRoots ?? [],
      requireWorkspaceRegistration:
        raw.requireWorkspaceRegistration ?? SCALAR_DEFAULTS.requireWorkspaceRegistration,
    },
  }
}
