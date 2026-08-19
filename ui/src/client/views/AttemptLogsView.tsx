/**
 * Attempt logs: live child history when the DAG parent session is current;
 * otherwise the stored result_json summary.
 * @module dsh-dag-view/client/views/AttemptLogsView
 */

import { useEffect, useState } from 'react'
import { getDict, type DagViewKey, type Locale } from '../locales.ts'
import styles from '../styles.module.css'

/** Folded history row: role/text, or a toolCall. */
export type HistoryEntry = {
  role?: string
  text?: string
  toolCall?: unknown
  seq?: number
}

/** Host adapter. Official RPC: subagents.history({ parentSessionId, childSessionId, mode, maxMessages?, beforeSeq? }). */
export interface LogSeam {
  getParentSessionId(): string | undefined
  fetchChildHistory(opts: {
    parentSessionId: string
    childSessionId: string
    maxMessages?: number
    beforeSeq?: number
  }): Promise<
    | { entries: Array<HistoryEntry>; hasMore?: boolean; oldestSeq?: number }
    | { error: string }
  >
}

export interface AttemptLogsViewProps {
  readonly locale: 'zh' | 'en'
  readonly attempt: { ordinal: number; child_session?: string; result_json?: string } | null
  readonly seam?: LogSeam
}

type LiveOk = { status: 'ok'; entries: HistoryEntry[]; hasMore: boolean; oldestSeq?: number }
type LiveState = { status: 'loading' } | { status: 'error' } | LiveOk

function tx(locale: Locale, key: DagViewKey): string {
  return getDict(locale)[key]
}

function pretty(value: unknown): string {
  try { return JSON.stringify(value, null, 2) ?? 'null' } catch { return String(value) }
}

function parsedJson(raw: string | undefined): unknown | undefined {
  if (raw === undefined || raw === '') return undefined
  try { return JSON.parse(raw) as unknown } catch { return undefined }
}

function liveIds(
  attempt: AttemptLogsViewProps['attempt'],
  seam: LogSeam | undefined,
): { parentId: string; childId: string } | undefined {
  const parentId = seam?.getParentSessionId()
  const childId = attempt?.child_session
  if (attempt === null || seam === undefined) return undefined
  if (!parentId || !childId) return undefined
  return { parentId, childId }
}

function Bubble({ entry }: { entry: HistoryEntry }): JSX.Element {
  if (entry.toolCall !== undefined) {
    return (
      <div className={styles.bubble}>
        <details><summary className={styles.toolCallLine}>tool</summary></details>
      </div>
    )
  }
  return (
    <div className={styles.bubble}>
      {entry.role ? <span className={styles.bubbleRole}>{entry.role}</span> : null}
      <div className={styles.bubbleText}>{entry.text ?? ''}</div>
    </div>
  )
}

/** Live child-session bubbles, or the stored attempt summary when the parent session is not current. */
export function AttemptLogsView({ locale, attempt, seam }: AttemptLogsViewProps): JSX.Element {
  const ids = liveIds(attempt, seam)
  const [live, setLive] = useState<LiveState>({ status: 'loading' })
  const [loadingOlder, setLoadingOlder] = useState(false)

  useEffect(() => {
    if (ids === undefined || seam === undefined) {
      setLive({ status: 'loading' })
      setLoadingOlder(false)
      return
    }
    const { parentId, childId } = ids
    let cancelled = false
    setLive({ status: 'loading' })
    void seam.fetchChildHistory({ parentSessionId: parentId, childSessionId: childId, maxMessages: 50 })
      .then((result) => {
        if (cancelled) return
        if ('error' in result) { setLive({ status: 'error' }); return }
        setLive({
          status: 'ok',
          entries: result.entries,
          hasMore: result.hasMore === true,
          oldestSeq: result.oldestSeq,
        })
      }, () => { if (!cancelled) setLive({ status: 'error' }) })
    return () => { cancelled = true }
  }, [ids?.parentId, ids?.childId, seam])

  if (attempt === null) {
    return <div className={styles.emptyState}>{tx(locale, 'empty.logs')}</div>
  }
  if (ids === undefined || live.status === 'error') {
    const parsed = parsedJson(attempt.result_json)
    return (
      <div className={styles.centerPanel}>
        <p className={styles.fallbackNote}>{tx(locale, 'logs.fallback')}</p>
        {parsed !== undefined ? <pre className={styles.jsonBlock}>{pretty(parsed)}</pre> : null}
      </div>
    )
  }
  if (live.status === 'loading') {
    return <div className={styles.loadingState}>{tx(locale, 'loading.logs')}</div>
  }

  const loadOlder = (): void => {
    if (seam === undefined || ids === undefined || loadingOlder || live.oldestSeq === undefined) return
    setLoadingOlder(true)
    void seam.fetchChildHistory({
      parentSessionId: ids.parentId,
      childSessionId: ids.childId,
      maxMessages: 50,
      beforeSeq: live.oldestSeq,
    }).then((result) => {
      setLoadingOlder(false)
      if ('error' in result) return
      setLive((cur) => cur.status !== 'ok' ? cur : {
        status: 'ok',
        entries: [...result.entries, ...cur.entries],
        hasMore: result.hasMore === true,
        oldestSeq: result.oldestSeq ?? cur.oldestSeq,
      })
    }, () => { setLoadingOlder(false) })
  }

  return (
    <div className={styles.logList}>
      {live.hasMore && live.oldestSeq !== undefined ? (
        <button type="button" className={styles.loadOlder} disabled={loadingOlder} onClick={loadOlder}>
          {tx(locale, 'logs.loadOlder')}
        </button>
      ) : null}
      {live.entries.length === 0
        ? <div className={styles.emptyState}>{tx(locale, 'empty.logs')}</div>
        : live.entries.map((entry, index) => <Bubble key={entry.seq ?? index} entry={entry} />)}
    </div>
  )
}
