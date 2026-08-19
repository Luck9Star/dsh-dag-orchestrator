/**
 * dag-view surface plugin, browser half: the OFFICIAL conversation tab.
 * The web shell exposes the 'conversation.view' slot ring (one list entry
 * per header tab — chat lives there at order 0); this half registers a
 * 'dag' entry at order 10 whose component (DagTabView) renders in the
 * conversation main area when selected. The old DOM-injection mount
 * (sidebar button + center-panel takeover) is gone — activation,
 * placement, and lifecycle are the shell's.
 *
 * Data still flows through this package's own host half's /dag-view/*
 * routes (same-origin fetch); the session link rides runs.planner_session
 * (written by the core plugin from the planning conversation's
 * exec.agent.session.id) through POST /dag-view/session-runs.
 *
 * inject: slots (platform module @deepseek-ai/dsh-client-ui-slots) for the
 * registration; sessions + locale from the browser runtime — value imports
 * stay platform-table-only (type-only imports elsewhere).
 * @module dsh-dag-view/client
 */

import type { ClientContext } from '@deepseek-ai/dsh-client-runtime/client'
import type {} from '@deepseek-ai/dsh-client-ui-conversation/client'
import type {} from '@deepseek-ai/dsh-client-locale/client'
import type { HistoryEntry, LogSeam } from './views/AttemptLogsView.tsx'
import { DagViewApi } from './api.ts'
import { dict, type DagViewKey, type Locale } from './locales.ts'
import { DagTabView, type DagTabContext } from './views/DagTabView.tsx'

/** Locale namespace this plugin owns. */
const NS = 'dag-view'

// Merge this namespace's key union into the locale registry's table (the
// task-board/aionui precedent): the typed register call below and the
// framework-synthesized `t` seat both resolve against the merged map.
declare module '@deepseek-ai/dsh-client-ui-slots' {
  interface LocaleNamespaceMap {
    /** dag-view surface copy. */
    'dag-view': DagViewKey
  }
}

/**
 * Required services: the slot registry, the session store, the locale
 * registry, and the connection API. Cordis guards property access behind
 * the inject declaration — LogSeam's OPTIONAL ctx.connection read still
 * requires the declaration, else apply() throws
 * "cannot get property ... without inject" at boot.
 */
export const inject: readonly string[] = ['slots', 'sessions', 'locale', 'connection']

/** Structural view of `ctx.connection.api.subagents.history` (optional service). */
interface SubagentsHistoryFace {
  readonly connection?: {
    readonly api?: {
      readonly subagents?: {
        readonly history?: (opts: {
          parentSessionId: string
          childSessionId: string
          mode?: string
          maxMessages?: number
          beforeSeq?: number
        }) => Promise<unknown>
      }
    }
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null ? value as Record<string, unknown> : undefined
}

function toHistoryEntry(item: unknown): HistoryEntry {
  const rec = asRecord(item)
  if (rec === undefined) return { text: String(item) }
  if (typeof rec.role === 'string' || typeof rec.text === 'string' || rec.toolCall !== undefined) {
    return {
      role: typeof rec.role === 'string' ? rec.role : undefined,
      text: typeof rec.text === 'string' ? rec.text : undefined,
      toolCall: rec.toolCall,
      seq: typeof rec.seq === 'number' ? rec.seq : undefined,
    }
  }
  const event = asRecord(rec.event)
  if (event === undefined) {
    return { text: '', seq: typeof rec.seq === 'number' ? rec.seq : undefined }
  }
  const data = asRecord(event.data)
  const text = typeof data?.text === 'string'
    ? data.text
    : typeof event.text === 'string'
      ? event.text
      : undefined
  return {
    role: typeof data?.role === 'string' ? data.role : undefined,
    text,
    toolCall: rec.view ?? data?.toolCall,
    seq: typeof event.seq === 'number' ? event.seq : typeof rec.seq === 'number' ? rec.seq : undefined,
  }
}

function mapHistory(raw: unknown): { entries: HistoryEntry[]; hasMore?: boolean; oldestSeq?: number } | { error: string } {
  const rec = asRecord(raw)
  if (rec === undefined) return { error: 'invalid history' }
  const result = asRecord(rec.result)
  if (result !== undefined) {
    if (result.ok === false) {
      const error = asRecord(result.error)
      return { error: typeof error?.message === 'string' ? error.message : 'history failed' }
    }
    if ('value' in result) return mapHistory(result.value)
  }
  const payload = asRecord(rec.value) ?? rec
  const list = Array.isArray(payload.entries)
    ? payload.entries
    : Array.isArray(payload.events)
      ? payload.events
      : Array.isArray(raw)
        ? raw
        : undefined
  if (list === undefined) return { error: 'invalid history' }
  return {
    entries: list.map(toHistoryEntry),
    hasMore: payload.hasMore === true,
    oldestSeq: typeof payload.oldestSeq === 'number' ? payload.oldestSeq : undefined,
  }
}

/** Build the live subagent-log seam from ctx.connection when the service is present. */
function buildLogSeam(ctx: ClientContext, getSessionId: () => string | undefined): LogSeam | undefined {
  const connection = (ctx as ClientContext & SubagentsHistoryFace).connection
  const history = connection?.api?.subagents?.history
  if (typeof history !== 'function') return undefined
  return {
    getParentSessionId: getSessionId,
    fetchChildHistory: async (opts) => {
      try {
        const raw = await history({
          parentSessionId: opts.parentSessionId,
          childSessionId: opts.childSessionId,
          mode: 'tail',
          maxMessages: opts.maxMessages,
          beforeSeq: opts.beforeSeq,
        })
        return mapHistory(raw)
      } catch (reason) {
        return { error: String(reason) }
      }
    },
  }
}

/** Document-language locale (the shell owns <html lang>; the registry mirrors it). */
function documentLocale(): Locale {
  const lang = typeof document !== 'undefined' ? document.documentElement.lang : 'zh'
  return lang.toLowerCase().startsWith('en') ? 'en' : 'zh'
}

/**
 * Client plugin body: register the locale dictionaries, then the
 * 'conversation.view' tab entry. The slot component receives its props
 * from the inject callback (the mount context: session getter, api,
 * locale, log seam); the returned disposer unregisters the tab.
 * @param ctx - client root context (services: slots, sessions, locale).
 */
export function apply(ctx: ClientContext): void {
  // Dictionaries through the official locale registry (bilingual balance
  // enforced there); zh/en keep the same flat keys the views read directly
  // (the tab label below is the registry-resolved surface).
  ctx.effect(() => ctx.locale.register(NS, { zh: dict.zh, en: dict.en }), 'dsh-dag-view: dictionaries')

  ctx.effect(() => {
    const getSessionId = (): string | undefined => {
      const snapshot = ctx.sessions.list.getSnapshot()
      const current = snapshot.current
      return typeof current === 'string' && current !== '' ? current : undefined
    }
    const context: DagTabContext = {
      getSessionId,
      api: new DagViewApi(),
      locale: documentLocale(),
      seam: buildLogSeam(ctx, getSessionId),
    }
    // The official tab: conversation header, next to Chat, order 10. The
    // label thunk resolves through the locale registry at read time
    // (resolveSlotLabel); the dictionaries above back the namespace.
    //
    // slots.inject(key, ...) is the declaration-wait guard (the task-board
    // precedent): 'conversation.view' is declared by ui-conversation's own
    // session entry, and registering into an undeclared slot throws. The
    // guard runs the registration the moment the declaration is live (and
    // re-runs it if the declaring plugin reloads), so a boot-order race
    // waits instead of crashing — package.json's dsh.client.inject lists
    // ui-conversation as a prerequisite edge, but that metadata is
    // informational, never a load-order guarantee.
    const disposeWait = ctx.slots.inject('conversation.view', () => ctx.slots.register(
      {
        name: 'conversation.view',
        id: 'dag',
        order: 10,
        label: () => 'DAG',
        locale: NS,
        inject: () => ({ dag: context }),
      },
      DagTabView,
    ))
    return disposeWait
  }, 'dsh-dag-view: conversation tab')
}
