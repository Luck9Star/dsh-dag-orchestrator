/**
 * App shell: runs list, or one run's graph + detail + tabbed panes.
 * @module dsh-dag-view/client/views/DagViewApp
 */

import { useEffect, useState } from 'react'
import type { ApiResult, DagViewApi } from '../api.ts'
import type { RunAggregate } from '../../core/types.ts'
import { getDict, type DagViewKey, type Locale } from '../locales.ts'
import styles from '../styles.module.css'
import { AttemptLogsView, type LogSeam } from './AttemptLogsView.tsx'
import { DagGraphView, type RunView } from './DagGraphView.tsx'
import { EventsView } from './EventsView.tsx'
import { OutputsView } from './OutputsView.tsx'
import { RunsListView } from './RunsListView.tsx'
import { TaskDetailView } from './TaskDetailView.tsx'

export interface DagViewAppProps {
  readonly api: DagViewApi
  readonly locale: Locale
  readonly seam?: LogSeam
  readonly refreshSignal?: number
  /** Preselect this run on mount (the tab embeds the tree with a run open). */
  readonly initialRunId?: string
  /** Notify when the user navigated back out of the run view (tab returns to its list). */
  readonly onExit?: () => void
}

type Tab = 'events' | 'outputs' | 'logs'
type LogAttempt = { ordinal: number; child_session?: string; result_json?: string }

const TABS: readonly Tab[] = ['events', 'outputs', 'logs']

function tx(locale: Locale, key: DagViewKey, params?: Record<string, string>): string {
  let text: string = getDict(locale)[key]
  if (params !== undefined) {
    for (const [name, value] of Object.entries(params)) {
      text = text.replaceAll(`{${name}}`, value)
    }
  }
  return text
}

/** Flatten /run aggregate into the snapshot DagGraphView renders. */
function toGraphRun(agg: RunAggregate): RunView {
  return { ...agg.run, spec: agg.spec, tasks: agg.tasks }
}

/** Shell: list all runs, or inspect one (graph, task detail, events/outputs/logs). */
export function DagViewApp({
  api,
  locale,
  seam,
  refreshSignal,
  initialRunId,
  onExit,
}: DagViewAppProps): JSX.Element {
  const [selectedRunId, setSelectedRunId] = useState<string | null>(initialRunId ?? null)
  const [run, setRun] = useState<ApiResult<RunAggregate> | undefined>(undefined)
  const [selectedTaskId, setSelectedTaskId] = useState<string | undefined>(undefined)
  const [tab, setTab] = useState<Tab>('events')
  const [logAttempt, setLogAttempt] = useState<LogAttempt | null>(null)
  const [lastUpdatedAt, setLastUpdatedAt] = useState<number | undefined>(undefined)
  const [reloadTick, setReloadTick] = useState(0)

  useEffect(() => {
    if (selectedRunId === null) {
      setRun(undefined)
      return
    }
    let cancelled = false
    setRun(undefined)
    void api.run(selectedRunId).then((result) => {
      if (cancelled) return
      setRun(result)
      setLastUpdatedAt(Date.now())
    })
    return () => {
      cancelled = true
    }
  }, [api, selectedRunId, refreshSignal, reloadTick])

  if (selectedRunId === null) {
    return (
      <RunsListView
        api={api}
        locale={locale}
        onOpenRun={setSelectedRunId}
        refreshSignal={refreshSignal}
      />
    )
  }

  const goBack = (): void => {
    setSelectedRunId(null)
    setSelectedTaskId(undefined)
    setTab('events')
    setRun(undefined)
    setLogAttempt(null)
    onExit?.()
  }

  const onViewLog = (ord: number): void => {
    const rows = run?.ok === true ? run.value.attempts : []
    const found = rows.find((a) => a.ordinal === ord && (selectedTaskId === undefined || a.task_id === selectedTaskId))
    setLogAttempt(found === undefined ? { ordinal: ord } : {
      ordinal: found.ordinal,
      child_session: found.child_session,
      result_json: found.failure !== undefined ? JSON.stringify(found.failure) : undefined,
    })
    setTab('logs')
  }

  return (
    <div className={styles.centerPanel}>
      <div className={styles.appHeader}>
        <button type="button" className={styles.backButton} onClick={goBack}>
          {tx(locale, 'action.back')}
        </button>
      </div>
      {run !== undefined && !run.ok ? (
        <div className={styles.errorState}>
          {tx(locale, 'error.withCode', { code: run.error.code, message: run.error.message })}
        </div>
      ) : null}
      {run?.ok === true ? (
        <DagGraphView
          run={toGraphRun(run.value)}
          locale={locale}
          selectedTaskId={selectedTaskId}
          onSelectTask={setSelectedTaskId}
          onRefresh={() => { setReloadTick((n) => n + 1) }}
          lastUpdatedAt={lastUpdatedAt}
        />
      ) : run === undefined ? (
        <div className={styles.loadingState}>{tx(locale, 'loading.graph')}</div>
      ) : null}
      {run?.ok === true ? (
        <TaskDetailView
          run={run.value}
          taskId={selectedTaskId ?? ''}
          locale={locale}
          onViewLog={onViewLog}
        />
      ) : null}
      <div className={styles.tabsBar}>
        {TABS.map((id) => (
          <button
            key={id}
            type="button"
            className={tab === id ? `${styles.tab} ${styles.tabActive}` : styles.tab}
            onClick={() => { setTab(id) }}
          >
            {tx(locale, `tab.${id}`)}
          </button>
        ))}
      </div>
      {tab === 'events' ? <EventsView api={api} runId={selectedRunId} locale={locale} /> : null}
      {tab === 'outputs' ? <OutputsView api={api} runId={selectedRunId} locale={locale} /> : null}
      {tab === 'logs' ? <AttemptLogsView locale={locale} attempt={logAttempt} seam={seam} /> : null}
    </div>
  )
}
