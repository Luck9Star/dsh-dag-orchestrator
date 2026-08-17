/**
 * dsh-dag-orchestrator — apply() assembly (T09, DESIGN §3 / §12.2 / §2.2).
 *
 * Single instance: DagStore / Executor / Admission / Engine are created
 * exactly once inside the apply() closure (the engine layer is the shared
 * singleton every tool talks to — DESIGN §3's layering: tools are a thin
 * stateless face, the engine holds all state, lib/dag-store.js is the only
 * persistence outlet, red lines R2/R7).
 *
 * apply() order (§3 diagram + §12.2 — the order IS the contract):
 *
 *   a. validateConfig (lib/config.js zod strict, `dag.config_invalid` on
 *      any unknown key / out-of-contract value — a typo like `dbpath`
 *      fails at apply time, never as a confusing runtime error later);
 *   b. assertSingleDshToolsInstance — the @deepseek-ai/dsh-tools
 *      dual-instance self-check (§2.2 C6 peer trap; probe carried over
 *      verbatim from dsh-plugin-subagents lib/index.js);
 *   c. createDagStore({path: dbPath}) — the store creates its parent
 *      directories (0700) and the db file itself (0600, wx); a foreign or
 *      wrong-version database refuses to open loud (dag.store_ownership /
 *      dag.store_version);
 *   d. reconcile(store) — the crash reconciliation (§12), STRICTLY BEFORE
 *      any tool registration (analysis §4-C4: the first model-visible call
 *      must already see a truthful state — chain verification and the
 *      never-dispatched auto-retry both happen while no tool can observe
 *      intermediate states);
 *   e. executor / admission / engine assembly over the SAME store
 *      singleton (executor binds ctx.subagents; execAgentProvider stays
 *      null in M1 — every dispatch carries the pumping Agent from the tool
 *      exec, per DESIGN §4.4's current-pumper ownership rule);
 *   f. tool registration per the register switches (M1: dag_plan /
 *      dag_status / dag_tick; M2: dag_control (T11) + dag_approve (T12));
 *   g. ctx.effect teardown (dispose in-flight subagents → engine.disposeAll
 *      → store.close; idempotent via a closed flag);
 *   h. optional autoTick Timer effect (T14, config.autoTickMs > 0): the
 *      no-dispatch reconcile on an interval — registered AFTER the main
 *      teardown effect so LIFO disposal clears the Timer BEFORE the store
 *      closes.
 *
 * Return value: ALWAYS undefined — Cordis treats the plugin callback's
 * return as a disposable (a non-nullable non-function fails real boots with
 * "TypeError: Invalid effect"). Introspection goes through the registered
 * tools (fake ctx in tests) or the internal modules, never the return.
 *
 * ---------------------------------------------------------------------------
 * @deepseek-ai/dsh-subagent IMPORT WHITELIST (red line, subagents 红线 12
 * 同款; mechanically enforced by the T24 lint): this file does NOT import
 * it. The single sanctioned consumer is lib/executor.js, which imports
 * exactly ONE member — assertSubagentMaxDepth — invoked before every
 * dispatch. Adding an import here (or anywhere else) needs the whitelist
 * updated first.
 * ---------------------------------------------------------------------------
 */

import { TOOL_RUNTIME_SCHEDULER } from '@deepseek-ai/dsh-tools'

import { validateConfig } from './config.js'
import { createDagStore } from './dag-store.js'
import { createEngine } from './engine.js'
import { createExecutor } from './executor.js'
import { createAdmission } from './admission.js'
import { createWorktreesSeam } from './worktrees-seam.js'
import { reconcile } from './recovery.js'
import { registerDagPlan } from './tools/dag-plan.js'
import { registerDagStatus } from './tools/dag-status.js'
import { registerDagTick } from './tools/dag-tick.js'
import { registerDagControl } from './tools/dag-control.js'
import { registerDagApprove } from './tools/dag-approve.js'

export const name = 'dsh-dag-orchestrator'

// `subagents` is bound through ctx (ctx.subagents.start at dispatch time —
// the executor's seam); only `tools` is a hard injection.
export const inject = ['tools']

/**
 * dsh-tools dual-instance self-check (§2.2 C6; the verbatim probe pattern
 * from dsh-plugin-subagents lib/index.js):
 *
 *   1. `ctx.tools[TOOL_RUNTIME_SCHEDULER] !== undefined` → same physical
 *      module — healthy;
 *   2. Symbol absent but ctx.tools looks like a real ToolRuntime
 *      (view + schemas) → a true second copy: logger.fatal + throw (apply
 *      failing beats every tool call dying after load with "Cannot read
 *      properties of undefined (reading 'prepare')"), pointing at the peer
 *      re-link (npm run setup:peer);
 *   3. anything else (fake ctx / an unseen host shape) → cannot reliably
 *      judge; warn only. Tests and non-standard hosts must not be killed
 *      by a false positive.
 */
function assertSingleDshToolsInstance(ctx) {
  const tools = ctx && ctx.tools
  if (!tools || typeof tools.register !== 'function') return
  if (tools[TOOL_RUNTIME_SCHEDULER] !== undefined) return // same physical module — healthy
  const looksLikeToolRuntime = typeof tools.view === 'function' && typeof tools.schemas === 'function'
  if (looksLikeToolRuntime) {
    const detail =
      'dsh-dag-orchestrator: detected a second @deepseek-ai/dsh-tools module instance — '
      + "every tool call from this plugin would die with \"Cannot read properties of undefined (reading 'prepare')\". "
      + 'Run npm run setup:peer (scripts/link-harness-dsh-tools.sh) in the dsh-dag-orchestrator package '
      + 'so its dsh-tools copy resolves to the live harness root, then restart dsh.'
    if (ctx.logger && typeof ctx.logger.fatal === 'function') ctx.logger.fatal(detail)
    throw new Error(detail)
  }
  if (ctx.logger && typeof ctx.logger.warn === 'function') {
    ctx.logger.warn(
      'dsh-dag-orchestrator: could not reliably verify the @deepseek-ai/dsh-tools single-instance invariant '
        + 'from this ctx (no scheduler symbol, no ToolRuntime shape) — run npm run setup:peer to check the dedupe link',
    )
  }
}

/**
 * Plugin entry (T09). See the module header for the assembly order.
 *
 * @param {object} ctx Cordis ctx (tools / subagents / effect + logger)
 * @param {object} [config] raw plugin config (validated here; zod strict)
 * @returns {Promise<undefined>} always undefined — the loader treats the
 *          plugin callback's return value as a disposable; a non-nullable
 *          non-function return fails real boots with "TypeError: Invalid
 *          effect"
 */
export async function apply(ctx, config = {}) {
  // a. config (zod strict, `dag.config_invalid` on any unknown key)
  const validated = validateConfig(config)
  if (!validated.ok) {
    throw new Error(`dag: dag.config_invalid — ${validated.error.message}`)
  }
  const cfg = validated.value

  // b. peer dual-instance defence (§2.2 C6)
  assertSingleDshToolsInstance(ctx)

  const log = ctx.logger ?? {}

  // c. store singleton. The db file itself (0600, wx) and its parent
  //    directories (0700, recursive) are the store's own discipline.
  const store = await createDagStore({ path: cfg.dbPath })

  let executor
  let engine
  let closed = false
  try {
    // d. crash reconciliation BEFORE any tool registration (§12.2; analysis
    //    §4-C4 — the ordering is a hard guarantee, not a convention: the
    //    first model-visible call must already see a truthful state).
    await reconcile(store, { logger: log })

    // e. engine assembly over the SAME store singleton. execAgentProvider
    //    is OMITTED in M1: every dispatch receives the pumping Agent from
    //    the tool exec (DESIGN §4.4 parent-ownership rule) — there is no
    //    ambient "current agent" at apply time to bind (the executor's
    //    optional provider seam stays reserved for a future host face).
    //    The worktrees seam (T15, DESIGN §11.2) is created HERE and probed
    //    ONCE (the apply-time probe of the "apply + every use" cadence) so
    //    the assembly log below can state availability honestly; the
    //    executor carries the seam handle for T16/T17 consumers, which
    //    re-probe at their own use time. The seam is stateless (no timers,
    //    no subscriptions) — teardown needs nothing beyond what already
    //    exists.
    const worktreesSeam = createWorktreesSeam(ctx, { logger: log })
    const worktreesProbe = worktreesSeam.probe()
    executor = createExecutor({
      ctxSubagents: ctx.subagents,
      store,
      config: cfg,
      logger: log,
      worktreesSeam,
    })
    const admission = createAdmission()
    engine = createEngine({ store, executor, admission, config: cfg, logger: log })

    // f. tool registration per the register switches (M1: the three tools).
    const deps = { engine, store, config: cfg }
    let registered = 0
    if (cfg.register.plan) {
      registerDagPlan(ctx, deps)
      registered += 1
    }
    if (cfg.register.status) {
      registerDagStatus(ctx, deps)
      registered += 1
    }
    if (cfg.register.tick) {
      registerDagTick(ctx, deps)
      registered += 1
    }
    if (cfg.register.control) {
      registerDagControl(ctx, deps)
      registered += 1
    }
    if (cfg.register.approve) {
      registerDagApprove(ctx, deps)
      registered += 1
    }

    // g. teardown (Cordis effect: the body runs now, the RETURNED function
    //    is the disposer — dispose in-flight subagents FIRST (they may
    //    still write through the open store's callers), then the engine's
    //    own sweep, then close the store; idempotent via the closed flag).
    ctx.effect(() => {
      return () => {
        if (closed) return
        closed = true
        try {
          for (const attemptId of executor.inFlightIds()) executor.dispose(attemptId)
        } catch (error) {
          log.warn?.(`dsh-dag-orchestrator: executor in-flight dispose threw (ignored): ${String(error?.message ?? error)}`)
        }
        try {
          engine.disposeAll?.()
        } catch (error) {
          log.warn?.(`dsh-dag-orchestrator: engine disposeAll threw (ignored): ${String(error?.message ?? error)}`)
        }
        try {
          store.close()
        } catch (error) {
          log.warn?.(`dsh-dag-orchestrator: store close threw (ignored): ${String(error?.message ?? error)}`)
        }
      }
    })

    // h. autoTick Timer (T14, DESIGN §5.5; config.autoTickMs, default 0 =
    //    off — O3's conservative opt-in). engine.autoTick is the
    //    NO-DISPATCH reconcile (no Timer-context exec.agent exists; the
    //    honest boundary lives in the method's comment). The Timer effect
    //    is registered AFTER the main teardown effect on purpose: Cordis
    //    runs effect disposers in reverse registration order (LIFO, the
    //    disposable contract in @deepseek-ai/cordis fiber.ts), so the
    //    unload sequence is clearInterval FIRST, main teardown (store
    //    close) second — a Timer pop after the store closed would hit
    //    dag.store_closed. The interval callback never throws: autoTick's
    //    own per-run catch + this Promise.resolve().catch → logger.warn
    //    keep the host alive (a Timer exception would otherwise kill the
    //    process). unref keeps the Timer from holding the event loop.
    if (cfg.autoTickMs > 0) {
      ctx.effect(() => {
        const timer = setInterval(() => {
          Promise.resolve(engine.autoTick()).catch((error) => {
            log.warn?.(`dag autoTick failed: ${String(error?.message ?? error)}`)
          })
        }, cfg.autoTickMs)
        if (typeof timer.unref === 'function') timer.unref()
        return () => clearInterval(timer)
      })
    }

    log.info?.(
      `dsh-dag-orchestrator: applied (${registered} tool${registered === 1 ? '' : 's'}; db ${cfg.dbPath}; `
        + `worktrees engine ${worktreesProbe.available ? 'available' : 'absent (agent-only DAG mode)'})`,
    )
  } catch (error) {
    // Assembly failed mid-way: the store we opened must not leak. The
    // effect was never registered, so close here (idempotent either way).
    if (!closed) {
      closed = true
      try {
        store.close()
      } catch {
        /* a failing close during a failing apply must not mask the error */
      }
    }
    throw error
  }

  return undefined
}
