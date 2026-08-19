/**
 * The conversation-tab body: the official 'conversation.view' slot entry's
 * component. Session-linked runs first (compact cards over the 10 s poll),
 * a divider, then the all-runs section; selecting a run renders the full
 * DagViewApp tree (graph + detail + events/logs/outputs) inline. An empty
 * state (no runs at all) shows the guidance card.
 *
 * Session identity comes from the framework's session standard kit
 * (`sessionId` prop — the slot is session-scoped) and from the mount
 * context's session store subscription (injected props); the LogSeam for
 * live subagent logs is threaded the same way.
 * @module dsh-dag-view/client/views/DagTabView
 */

import { useEffect, useState } from 'react'
import type { ApiResult, DagViewApi } from '../api.ts'
import { formatCounts, relativeTime, stateTone } from '../../core/format.ts'
import type { Locale } from '../locales.ts'
import { getDict, type DagViewKey } from '../locales.ts'
import styles from '../styles.module.css'
import type { LogSeam } from './AttemptLogsView.tsx'
import { DagViewApp } from './DagViewApp.tsx'

/** Everything the slot's inject callback hands the component. */
export interface DagTabContext {
  /** Current-session getter (ctx.sessions.list store read). */
  readonly getSessionId: () => string | undefined
  /** Envelope API over the /dag-view routes. */
  readonly api: DagViewApi
  /** Active locale (document-language-derived, as the mounted tree uses). */
  readonly locale: Locale
  /** Live subagent-log seam (built from ctx.connection when present). */
  readonly seam?: LogSeam
}

export interface DagTabViewProps {
  /** Inject share: the mount context created in client index.ts. */
  readonly dag: DagTabContext
  /** Framework standard kit: the resolved session id (session-scope slot). */
  readonly sessionId?: string
}

const LIST_POLL_MS = 10_000

function tx(locale: Locale, key: DagViewKey, params?: Record<string, string>): string {
  let text: string = getDict(locale)[key]
  if (params !== undefined) {
    for (const [name, value] of Object.entries(params)) {
      text = text.replaceAll(`{${name}}`, value)
    }
  }
  return text
}

function stateLabel(locale: Locale, state: string): string {
  const key = `state.${state}` as DagViewKey
  const dict = getDict(locale)
  return key in dict ? dict[key] : state
}

function toneClass(state: string): string {
  const key = stateTone(state)
  return styles[key] ?? key
}

type ListState =
  | { status: 'loading' }
  | { status: 'error'; error: { code: string; message: string } }
  | { status: 'ok'; sessionRuns: readonly RunCard[]; allRuns: readonly RunCard[] }

/** One compact card row (summary-row shape from both routes). */
interface RunCard {
  readonly run_id: string
  readonly name: string
  readonly state: string
  readonly counts: Record<string, number>
  readonly updated_at: number
}

function toCards(runs: readonly unknown[] | undefined): readonly RunCard[] {
  if (runs === undefined) return []
  return runs.map((raw) => {
    const row = raw as Record<string, unknown>
    return {
      run_id: String(row.run_id),
      name: String(row.name),
      state: String(row.state),
      counts: (row.counts ?? {}) as Record<string, number>,
      updated_at: typeof row.updated_at === 'number' ? row.updated_at : 0,
    }
  })
}

function SessionRunCards({
  runs, locale, onOpen,
}: {
  runs: readonly RunCard[]
  locale: Locale
  onOpen: (runId: string) => void
}): JSX.Element {
  return (
    <div className={styles.tabCardList}>
      {runs.map((run) => (
        <button
          type="button"
          key={run.run_id}
          className={styles.tabCard}
          data-run-id={run.run_id}
          onClick={() => { onOpen(run.run_id) }}
        >
          <span className={styles.runName}>{run.name}</span>
          <span className={`${styles.stateBadge} ${toneClass(run.state)}`}>
            {stateLabel(locale, run.state)}
          </span>
          <span className={styles.countsSummary}>{formatCounts(run.counts, locale)}</span>
          <span className={styles.updatedAgo}>
            {relativeTime(Date.now(), run.updated_at, locale)}
          </span>
        </button>
      ))}
    </div>
  )
}

function EmptyCard({ locale }: { locale: Locale }): JSX.Element {
  return (
    <div className={styles.emptyCard} data-dag-empty-card="">
      <div className={styles.emptyCardTitle}>{tx(locale, 'empty.tabTitle')}</div>
      <p className={styles.emptyCardBody}>{tx(locale, 'empty.tabBody')}</p>
      <p className={styles.emptyCardHint}>{tx(locale, 'empty.tabHint')}</p>
    </div>
  )
}

/** The DAG conversation tab: session runs first, all runs below, run view inline. */
export function DagTabView({ dag, sessionId }: DagTabViewProps): JSX.Element {
  const { api, locale, seam } = dag
  const [currentSession, setCurrentSession] = useState<string | undefined>(sessionId)
  const [list, setList] = useState<ListState>({ status: 'loading' })
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null)

  // Follow the shell's session selection through the injected getter; the
  // framework sessionId prop seeds the first render (and covers hosts
  // without an inject-provided store).
  useEffect(() => {
    setCurrentSession(sessionId)
  }, [sessionId])

  // Session-linked + all-runs poll (10 s while the tab is mounted and the
  // document visible; the shell only mounts the active view).
  useEffect(() => {
    let cancelled = false
    const load = (): void => {
      const session = dag.getSessionId() ?? sessionId
      const allPromise = api.runs()
      const sessionPromise = session === undefined
        ? Promise.resolve({ ok: true as const, value: { runs: [] } })
        : api.sessionRuns(session)
      void Promise.all([sessionPromise, allPromise]).then(([sessionResult, allResult]) => {
        if (cancelled) return
        if (!allResult.ok) {
          setList({ status: 'error', error: allResult.error })
          return
        }
        if (!sessionResult.ok) {
          // Session rows are an enhancement: degrade to the all-runs arm.
          setList({ status: 'ok', sessionRuns: [], allRuns: toCards(allResult.value.runs) })
          return
        }
        setList({
          status: 'ok',
          sessionRuns: toCards(sessionResult.value.runs),
          allRuns: toCards(allResult.value.runs),
        })
      })
    }
    setList({ status: 'loading' })
    load()
    const timer = setInterval(() => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') return
      load()
    }, LIST_POLL_MS)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [api, dag, sessionId])

  // A run is open: the full view tree renders inline (DagViewApp keeps its
  // own header/back button — back returns to the tab list).
  if (selectedRunId !== null) {
    return (
      <div className={styles.tabRoot}>
        <DagViewApp
          api={api}
          locale={locale}
          seam={seam}
          initialRunId={selectedRunId}
          onExit={(): void => { setSelectedRunId(null) }}
        />
      </div>
    )
  }

  if (list.status === 'loading') {
    return (
      <div className={styles.tabRoot}>
        <div className={styles.loadingState}>{tx(locale, 'loading.runs')}</div>
      </div>
    )
  }
  if (list.status === 'error') {
    return (
      <div className={styles.tabRoot}>
        <div className={styles.errorState}>
          {tx(locale, 'error.withCode', { code: list.error.code, message: list.error.message })}
        </div>
      </div>
    )
  }

  const { sessionRuns, allRuns } = list
  if (allRuns.length === 0) {
    return (
      <div className={styles.tabRoot}>
        <EmptyCard locale={locale} />
      </div>
    )
  }

  return (
    <div className={styles.tabRoot}>
      {sessionRuns.length > 0 ? (
        <>
          <div className={styles.sectionTitle}>{tx(locale, 'section.sessionRuns')}</div>
          <SessionRunCards runs={sessionRuns} locale={locale} onOpen={setSelectedRunId} />
        </>
      ) : currentSession === undefined ? null : (
        <div className={styles.sectionEmpty}>{tx(locale, 'empty.tab')}</div>
      )}
      <div className={styles.sectionTitle}>{tx(locale, 'section.allRuns')}</div>
      <SessionRunCards runs={allRuns} locale={locale} onOpen={setSelectedRunId} />
    </div>
  )
}
