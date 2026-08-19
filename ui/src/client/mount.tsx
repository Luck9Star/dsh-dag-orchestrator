/**
 * Sidebar entry + center-column takeover. One MutationObserver waits for
 * `[data-pane="sidebar"]` and `[data-pane="conversation"]`, injects a
 * plain-DOM button and a React root the shell never manages, and
 * arbitrates visibility via `html[data-dsh-dagview-active]` plus
 * `dsh-panel-activate`.
 * @module dsh-dag-view/client/mount
 */

import { createRoot, type Root } from 'react-dom/client'
import { DagViewApi } from './api.ts'
import { getDict, type Locale } from './locales.ts'
import css from './styles.module.css'
import { DagViewApp } from './views/DagViewApp.tsx'
import type { HistoryEntry, LogSeam } from './views/AttemptLogsView.tsx'

/** Stable data attribute identifying the injected sidebar entry row. */
export const ENTRY_SELECTOR = '[data-dsh-dagview-entry]'

/** Stable data attribute identifying the injected center panel container. */
export const PANEL_SELECTOR = '[data-dsh-dagview-panel]'

/** The html-level attribute toggling center-panel visibility. */
export const ACTIVE_ATTRIBUTE = 'data-dsh-dagview-active'

/** Cross-plugin activation event name. */
const ACTIVATE_EVENT = 'dsh-panel-activate'

/** This panel's name in `dsh-panel-activate` detail. */
const PANEL_NAME = 'dag-view'

/** Sibling center-panel html attributes to evict when this panel opens. */
const SIBLING_ACTIVE_ATTRIBUTES = ['data-dsh-taskboard-active', 'data-dsh-ssh-active'] as const

const LIST_POLL_MS = 10_000
const RUN_POLL_MS = 2_000

/** Three nodes joined by edges — a small DAG glyph, no external assets. */
const ICON = '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="1.5" y="6" width="4" height="4" rx="0.6"/><rect x="10.5" y="1.5" width="4" height="4" rx="0.6"/><rect x="10.5" y="10.5" width="4" height="4" rx="0.6"/><line x1="5.5" y1="8" x2="10.5" y2="3.5"/><line x1="5.5" y1="8" x2="10.5" y2="12.5"/></svg>'

/** Optional host context createMount reads; every field is structural. */
export interface DagViewMountContext {
  readonly api?: DagViewApi
  readonly locale?: Locale
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
  readonly session?: string | { readonly id?: string }
}

/** The mounted surface: everything createMount owns, with its stop(). */
export interface DagViewMount {
  /** Start the mount (idempotent). */
  start(): void
  /** Stop the mount and clean up every observer and DOM node. */
  stop(): void
  /** Whether the mount is currently started. */
  readonly started: boolean
}

/** Whether the dag-view panel is currently the active center view. */
export function isPanelActive(): boolean {
  return typeof document !== 'undefined' && document.documentElement.hasAttribute(ACTIVE_ATTRIBUTE)
}

/** Activate the dag-view center panel and evict sibling panels. */
export function activatePanel(): void {
  if (typeof document === 'undefined') return
  for (const name of SIBLING_ACTIVE_ATTRIBUTES) {
    document.documentElement.removeAttribute(name)
  }
  document.documentElement.setAttribute(ACTIVE_ATTRIBUTE, '')
  document.dispatchEvent(new CustomEvent(ACTIVATE_EVENT, { detail: { panel: PANEL_NAME } }))
}

/** Deactivate the dag-view center panel. */
export function deactivatePanel(): void {
  if (typeof document === 'undefined') return
  document.documentElement.removeAttribute(ACTIVE_ATTRIBUTE)
}

/** Toggle the dag-view center panel. */
export function togglePanel(): boolean {
  const active = !isPanelActive()
  if (active) activatePanel()
  else deactivatePanel()
  return active
}

function documentLocale(): Locale {
  const lang = typeof document !== 'undefined' ? document.documentElement.lang : 'zh'
  return lang.toLowerCase().startsWith('en') ? 'en' : 'zh'
}

function resolveLocale(ctx: DagViewMountContext | undefined): Locale {
  return ctx?.locale === 'en' || ctx?.locale === 'zh' ? ctx.locale : documentLocale()
}

function parentSessionId(ctx: DagViewMountContext | undefined): string | undefined {
  if (ctx === undefined) return undefined
  const session = ctx.session
  if (typeof session === 'string') return session === '' ? undefined : session
  if (session !== undefined && typeof session.id === 'string' && session.id !== '') return session.id
  return undefined
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

/** Build a LogSeam from `ctx.connection.api.subagents.history` when present. */
export function buildLogSeam(ctx: DagViewMountContext | undefined): LogSeam | undefined {
  const history = ctx?.connection?.api?.subagents?.history
  if (typeof history !== 'function') return undefined
  return {
    getParentSessionId: () => parentSessionId(ctx),
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

function activatingPanel(detail: unknown): string | undefined {
  if (typeof detail === 'string') return detail
  const rec = asRecord(detail)
  return typeof rec?.panel === 'string' ? rec.panel : undefined
}

function isDocumentLike(value: object): value is Document {
  return 'nodeType' in value && (value as { nodeType: unknown }).nodeType === 9
}

function asMountCtx(value: DagViewMountContext | Document | undefined): DagViewMountContext | undefined {
  if (value === undefined || isDocumentLike(value)) return undefined
  return value
}

function scopeOf(value: DagViewMountContext | Document | undefined): Document {
  return value !== undefined && isDocumentLike(value) ? value : document
}

function entryClassName(open: boolean): string {
  return open ? `${css.entry} ${css.sidebarButtonActive}` : css.entry
}

function applyEntryActive(entry: HTMLElement, open: boolean): void {
  entry.className = entryClassName(open)
  if (open) entry.dataset.active = 'true'
  else delete entry.dataset.active
}

function buildEntry(locale: Locale, onClick: () => void): HTMLButtonElement {
  const entry = document.createElement('button')
  entry.type = 'button'
  entry.dataset.dshDagviewEntry = ''
  entry.className = css.entry
  const label = getDict(locale)['entry.label']
  entry.setAttribute('aria-label', label)
  entry.innerHTML = `<span class="${css.entryIcon}">${ICON}</span><span class="${css.entryLabel}">${label}</span>`
  entry.addEventListener('click', () => { onClick() })
  return entry
}

/**
 * Mount the sidebar entry: a plain button appended into the sidebar shell
 * (`[data-pane="sidebar"]`), waiting for the shell to render and
 * self-healing on later React re-renders.
 * @param onClick - invoked when the entry row is clicked (view toggle).
 * @returns disposer removing the entry and its observers.
 */
export function mountSidebarEntry(onClick: () => void): () => void {
  const entry = buildEntry(documentLocale(), onClick)

  let sidebar: HTMLElement | undefined
  let placed = false

  const placeEntry = (): boolean => {
    if (sidebar === undefined || !sidebar.isConnected) {
      sidebar = document.querySelector<HTMLElement>('[data-pane="sidebar"], [class*="sidebarCol"]') ?? undefined
    }
    if (sidebar === undefined) return false
    if (!sidebar.contains(entry)) sidebar.appendChild(entry)
    return true
  }

  const tryPlace = (): void => {
    if (placed) return
    placed = placeEntry()
    if (placed) {
      sidebarObserver.observe(sidebar!, { childList: true, subtree: true })
      waitObserver.disconnect()
    }
  }

  const waitObserver = new MutationObserver(() => { tryPlace() })
  waitObserver.observe(document.body, { childList: true, subtree: true })

  const sidebarObserver = new MutationObserver(() => {
    if (!placed) return
    if (sidebar === undefined || !sidebar.isConnected || !sidebar.contains(entry)) {
      placed = placeEntry()
    }
  })

  tryPlace()

  return () => {
    waitObserver.disconnect()
    sidebarObserver.disconnect()
    entry.remove()
  }
}

/**
 * Mount the center panel: a container appended inside the conversation
 * pane carrying a React root with DagViewApp.
 * @returns disposer unmounting the React root and removing the container.
 */
export function mountCenterPanel(): () => void {
  const container = document.createElement('div')
  container.dataset.dshDagviewPanel = ''
  container.className = css.panel
  const root: Root = createRoot(container)
  root.render(<DagViewApp api={new DagViewApi()} locale={documentLocale()} />)
  const host = document.querySelector<HTMLElement>('[data-pane="conversation"]') ?? document.body
  host.appendChild(container)
  return () => {
    root.unmount()
    container.remove()
    deactivatePanel()
  }
}

/**
 * Create the dag-view mount: sidebar entry + center panel, polling, and
 * panel arbitration. `ctx` may be omitted (index.ts / tests) or a Document
 * (legacy target) — both keep existing callers compiling.
 * @param ctx - host context (api / locale / connection / session) or Document.
 * @returns the mount handle with start/stop.
 */
export function createMount(ctx?: DagViewMountContext | Document): DagViewMount {
  const scope = scopeOf(ctx)
  const mountCtx = asMountCtx(ctx)
  const api = mountCtx?.api ?? new DagViewApi()
  const locale = resolveLocale(mountCtx)
  const seam = buildLogSeam(mountCtx)

  let started = false
  let observer: MutationObserver | undefined
  let entry: HTMLButtonElement | undefined
  let panel: HTMLDivElement | undefined
  let root: Root | undefined
  let refreshSignal = 0
  let listTimer: ReturnType<typeof setInterval> | undefined
  let runTimer: ReturnType<typeof setInterval> | undefined
  let onActivate: ((event: Event) => void) | undefined
  let onVisibility: (() => void) | undefined

  const renderApp = (): void => {
    if (root === undefined) return
    root.render(
      <DagViewApp
        api={api}
        locale={locale}
        seam={seam}
        refreshSignal={refreshSignal}
      />,
    )
  }

  const syncEntry = (): void => {
    if (entry !== undefined) applyEntryActive(entry, isPanelActive())
  }

  const canPoll = (): boolean => {
    return started && isPanelActive() && typeof document !== 'undefined' && document.visibilityState === 'visible'
  }

  const isOpenRun = (): boolean => {
    return panel !== undefined && panel.querySelector(`.${css.tabsBar}`) !== null
  }

  const clearPolling = (): void => {
    if (listTimer !== undefined) {
      clearInterval(listTimer)
      listTimer = undefined
    }
    if (runTimer !== undefined) {
      clearInterval(runTimer)
      runTimer = undefined
    }
  }

  const bump = (): void => {
    refreshSignal += 1
    renderApp()
  }

  const syncPolling = (): void => {
    clearPolling()
    if (!canPoll()) return
    listTimer = setInterval(() => {
      if (!canPoll()) {
        syncPolling()
        return
      }
      if (!isOpenRun()) bump()
    }, LIST_POLL_MS)
    runTimer = setInterval(() => {
      if (!canPoll()) {
        syncPolling()
        return
      }
      if (isOpenRun()) bump()
    }, RUN_POLL_MS)
  }

  const hide = (): void => {
    if (!isPanelActive()) return
    deactivatePanel()
    syncEntry()
    syncPolling()
  }

  const onEntryClick = (): void => {
    if (isPanelActive()) deactivatePanel()
    else activatePanel()
    syncEntry()
    syncPolling()
  }

  const place = (): void => {
    const sidebar = scope.querySelector<HTMLElement>('[data-pane="sidebar"]')
      ?? scope.querySelector<HTMLElement>('[class*="sidebarCol"]')
    if (sidebar !== null) {
      if (entry === undefined) entry = buildEntry(locale, onEntryClick)
      if (!sidebar.contains(entry)) sidebar.appendChild(entry)
      syncEntry()
    }

    const conversation = scope.querySelector<HTMLElement>('[data-pane="conversation"]')
    const host = conversation ?? scope.body ?? document.body
    if (host !== null) {
      if (panel === undefined || !panel.isConnected || !host.contains(panel)) {
        if (root !== undefined) {
          root.unmount()
          root = undefined
        }
        panel?.remove()
        panel = scope.createElement('div')
        panel.dataset.dshDagviewPanel = ''
        panel.className = css.panel
        host.appendChild(panel)
        root = createRoot(panel)
        renderApp()
      }
    }
  }

  const start = (): void => {
    if (started) return
    started = true
    onActivate = (event: Event): void => {
      const name = activatingPanel((event as CustomEvent).detail)
      if (name !== undefined && name !== PANEL_NAME) hide()
    }
    onVisibility = (): void => { syncPolling() }
    document.addEventListener(ACTIVATE_EVENT, onActivate)
    document.addEventListener('visibilitychange', onVisibility)
    observer = new MutationObserver(() => { place() })
    const rootNode = scope.body ?? scope.documentElement
    observer.observe(rootNode, { childList: true, subtree: true })
    place()
    syncPolling()
  }

  const stop = (): void => {
    if (!started) return
    started = false
    clearPolling()
    observer?.disconnect()
    observer = undefined
    if (onActivate !== undefined) {
      document.removeEventListener(ACTIVATE_EVENT, onActivate)
      onActivate = undefined
    }
    if (onVisibility !== undefined) {
      document.removeEventListener('visibilitychange', onVisibility)
      onVisibility = undefined
    }
    root?.unmount()
    root = undefined
    entry?.remove()
    entry = undefined
    panel?.remove()
    panel = undefined
    deactivatePanel()
  }

  return {
    start,
    stop,
    get started(): boolean {
      return started
    },
  }
}
