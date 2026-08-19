/**
 * Event tail: incremental poll, newest at the bottom, auto-follow until scroll-away.
 * @module dsh-dag-view/client/views/EventsView
 */

import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import type { DagViewApi } from '../api.ts'
import type { EventRow, EventsView as EventsPayload } from '../../core/types.ts'
import { getDict, type DagViewKey, type Locale } from '../locales.ts'
import styles from '../styles.module.css'

export interface EventsViewProps {
  readonly api: Pick<DagViewApi, 'events'>
  readonly runId: string
  readonly locale: Locale
  readonly taskFilter?: string
}

type LoadState =
  | { status: 'loading' }
  | { status: 'error'; error: { code: string; message: string } }
  | { status: 'ok' }

function tx(locale: Locale, key: DagViewKey, params?: Record<string, string>): string {
  let text: string = getDict(locale)[key]
  if (params !== undefined) {
    for (const [name, value] of Object.entries(params)) {
      text = text.replaceAll(`{${name}}`, value)
    }
  }
  return text
}

function clock(at: number): string {
  const d = new Date(at)
  const pad = (n: number): string => String(n).padStart(2, '0')
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

function pretty(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2) ?? 'null'
  } catch {
    return String(value)
  }
}

function asEvents(value: unknown): EventRow[] {
  if (Array.isArray(value)) return value as EventRow[]
  if (typeof value === 'object' && value !== null && Array.isArray((value as EventsPayload).events)) {
    return [...(value as EventsPayload).events]
  }
  return []
}

/** Incremental event tail for one run; stays pinned to the newest row until the user scrolls up. */
export function EventsView({ api, runId, locale, taskFilter }: EventsViewProps): JSX.Element {
  const listRef = useRef<HTMLDivElement>(null)
  const maxSeqSeen = useRef(0)
  const [paused, setPaused] = useState(false)
  const [hidden, setHidden] = useState(
    () => typeof document !== 'undefined' && document.visibilityState !== 'visible',
  )
  const [load, setLoad] = useState<LoadState>({ status: 'loading' })
  const [rows, setRows] = useState<readonly EventRow[]>([])

  useEffect(() => {
    maxSeqSeen.current = 0
    setRows([])
    setLoad({ status: 'loading' })
    setPaused(false)
  }, [runId, taskFilter])

  useEffect(() => {
    const onVis = (): void => {
      setHidden(document.visibilityState !== 'visible')
    }
    document.addEventListener('visibilitychange', onVis)
    return () => {
      document.removeEventListener('visibilitychange', onVis)
    }
  }, [])

  useEffect(() => {
    if (paused || hidden) return
    let cancelled = false
    let busy = false
    const poll = async (): Promise<void> => {
      if (cancelled || busy) return
      busy = true
      const result = await api.events(runId, {
        after_seq: maxSeqSeen.current,
        task_id: taskFilter,
        limit: 100,
      })
      busy = false
      if (cancelled) return
      if (!result.ok) {
        setLoad((cur) => (cur.status === 'ok' ? cur : { status: 'error', error: result.error }))
        return
      }
      const incoming = asEvents(result.value)
      if (incoming.length > 0) {
        setRows((prev) => [...prev, ...incoming])
        for (const ev of incoming) {
          if (ev.seq > maxSeqSeen.current) maxSeqSeen.current = ev.seq
        }
      }
      setLoad({ status: 'ok' })
    }
    void poll()
    const timer = setInterval(() => { void poll() }, 2000)
    return () => {
      cancelled = true
      clearInterval(timer)
    }
  }, [api, runId, taskFilter, paused, hidden])

  useLayoutEffect(() => {
    const el = listRef.current
    if (el !== null && !paused) el.scrollTop = el.scrollHeight
  }, [rows, paused])

  function onScroll(): void {
    const el = listRef.current
    if (el === null) return
    const away = el.scrollHeight - el.scrollTop - el.clientHeight > 24
    setPaused(away)
  }

  let body: JSX.Element
  if (load.status === 'loading') {
    body = <div className={styles.loadingState}>{tx(locale, 'loading.events')}</div>
  } else if (load.status === 'error') {
    body = (
      <div className={styles.errorState}>
        {tx(locale, 'error.withCode', { code: load.error.code, message: load.error.message })}
      </div>
    )
  } else if (rows.length === 0) {
    body = <div className={styles.emptyState}>{tx(locale, 'empty.events')}</div>
  } else {
    body = (
      <div ref={listRef} className={styles.eventsList} onScroll={onScroll}>
        {rows.map((ev) => (
          <div key={ev.seq} className={styles.eventRow}>
            <span className={styles.eventSeq}>{ev.seq}</span>
            <span className={styles.eventType}>{ev.type}</span>
            <span className={styles.eventTime}>{clock(ev.at)}</span>
            {ev.task_id !== undefined ? <span className={styles.taskChip}>{ev.task_id}</span> : null}
            <details className={styles.payloadDetails}>
              <pre className={styles.jsonBlock}>{pretty(ev.payload)}</pre>
            </details>
          </div>
        ))}
      </div>
    )
  }

  return <div className={styles.centerPanel}>{body}</div>
}
