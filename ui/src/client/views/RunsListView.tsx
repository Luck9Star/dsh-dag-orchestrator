/**
 * Runs list: envelope-driven table of run summary cards.
 * @module dsh-dag-view/client/views/RunsListView
 */

import { useEffect, useState } from 'react'
import type { ApiResult } from '../api.ts'
import { formatCounts, relativeTime, stateTone } from '../../core/format.ts'
import type { RunsView, RunSummary } from '../../core/types.ts'
import { getDict, type DagViewKey, type Locale } from '../locales.ts'
import styles from '../styles.module.css'

/** Structural client the list needs; `DagViewApi` satisfies this. */
export type DagApi = {
  runs(): Promise<ApiResult<RunsView>>
}

export interface RunsListViewProps {
  readonly api: DagApi
  readonly locale: Locale
  readonly onOpenRun: (runId: string) => void
  /** Increment to force a reload from the parent. */
  readonly refreshSignal?: number
}

type LoadState =
  | { status: 'loading' }
  | { status: 'error'; error: { code: string; message: string } }
  | { status: 'ok'; runs: readonly RunSummary[] }

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

function shortId(id: string): string {
  return id.slice(0, 8)
}

/** Load all runs and render summary cards; row click opens a run. */
export function RunsListView({
  api,
  locale,
  onOpenRun,
  refreshSignal,
}: RunsListViewProps): JSX.Element {
  const [load, setLoad] = useState<LoadState>({ status: 'loading' })
  const [loadedAt, setLoadedAt] = useState<number | undefined>(undefined)
  const [localTick, setLocalTick] = useState(0)

  useEffect(() => {
    let cancelled = false
    setLoad({ status: 'loading' })
    void api.runs().then((result) => {
      if (cancelled) return
      if (result.ok) {
        setLoad({ status: 'ok', runs: result.value.runs })
      } else {
        setLoad({ status: 'error', error: result.error })
      }
      setLoadedAt(Date.now())
    })
    return () => {
      cancelled = true
    }
  }, [api, refreshSignal, localTick])

  let body: JSX.Element
  if (load.status === 'loading') {
    body = <div className={styles.loadingState}>{tx(locale, 'loading.runs')}</div>
  } else if (load.status === 'error') {
    body = (
      <div className={styles.errorState}>
        {tx(locale, 'error.withCode', { code: load.error.code, message: load.error.message })}
      </div>
    )
  } else if (load.runs.length === 0) {
    body = <div className={styles.emptyState}>{tx(locale, 'empty.runs')}</div>
  } else {
    body = (
      <div className={styles.runsTable}>
        {load.runs.map((run) => (
          <div
            key={run.run_id}
            className={styles.runsRow}
            onClick={() => { onOpenRun(run.run_id) }}
          >
            <span className={styles.runName}>{run.name}</span>
            <span className={styles.runIdShort}>{shortId(run.run_id)}</span>
            <span className={`${styles.stateBadge} ${toneClass(run.state)}`}>
              {stateLabel(locale, run.state)}
            </span>
            <span className={styles.countsSummary}>{formatCounts(run.counts, locale)}</span>
            <span className={styles.updatedAgo}>
              {relativeTime(Date.now(), run.updated_at, locale)}
            </span>
          </div>
        ))}
      </div>
    )
  }

  return (
    <div className={styles.centerPanel}>
      <div className={styles.refreshRow}>
        <button
          type="button"
          className={styles.backButton}
          onClick={() => { setLocalTick((n) => n + 1) }}
        >
          {tx(locale, 'action.refresh')}
        </button>
        {loadedAt !== undefined ? (
          <span className={styles.refreshTime}>
            {tx(locale, 'refresh.updated', { t: relativeTime(Date.now(), loadedAt, locale) })}
          </span>
        ) : null}
      </div>
      {body}
    </div>
  )
}
