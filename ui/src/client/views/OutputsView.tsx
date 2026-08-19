/**
 * Task outputs: list of validated output rows from the run aggregate.
 * @module dsh-dag-view/client/views/OutputsView
 */

import { useEffect, useState } from 'react'
import type { DagViewApi } from '../api.ts'
import type { OutputRow } from '../../core/types.ts'
import { getDict, type DagViewKey, type Locale } from '../locales.ts'
import styles from '../styles.module.css'

export interface OutputsViewProps {
  readonly api: Pick<DagViewApi, 'run'>
  readonly runId: string
  readonly locale: 'zh' | 'en'
  readonly refreshSignal?: number
}

type LoadState =
  | { status: 'loading' }
  | { status: 'error'; error: { code: string; message: string } }
  | { status: 'ok'; outputs: readonly OutputRow[] }

function tx(locale: Locale, key: DagViewKey, params?: Record<string, string>): string {
  let text: string = getDict(locale)[key]
  if (params !== undefined) {
    for (const [name, value] of Object.entries(params)) {
      text = text.replaceAll(`{${name}}`, value)
    }
  }
  return text
}

function pretty(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2) ?? 'null'
  } catch {
    return String(value)
  }
}

/** Load one run aggregate and render its outputs list. */
export function OutputsView({ api, runId, locale, refreshSignal }: OutputsViewProps): JSX.Element {
  const [load, setLoad] = useState<LoadState>({ status: 'loading' })
  const [copied, setCopied] = useState<string | undefined>(undefined)

  useEffect(() => {
    let cancelled = false
    setLoad({ status: 'loading' })
    void api.run(runId).then((result) => {
      if (cancelled) return
      if (result.ok) setLoad({ status: 'ok', outputs: result.value.outputs })
      else setLoad({ status: 'error', error: result.error })
    })
    return () => { cancelled = true }
  }, [api, runId, refreshSignal])

  const copy = (key: string, value: unknown): void => {
    try {
      void navigator.clipboard.writeText(pretty(value)).then(() => {
        setCopied(key)
        window.setTimeout(() => { setCopied((cur) => (cur === key ? undefined : cur)) }, 1500)
      })
    } catch {
      // clipboard may be unavailable
    }
  }

  let body: JSX.Element
  if (load.status === 'loading') {
    body = <div className={styles.loadingState}>{tx(locale, 'loading.outputs')}</div>
  } else if (load.status === 'error') {
    body = (
      <div className={styles.errorState}>
        {tx(locale, 'error.withCode', { code: load.error.code, message: load.error.message })}
      </div>
    )
  } else if (load.outputs.length === 0) {
    body = <div className={styles.emptyState}>{tx(locale, 'empty.outputs')}</div>
  } else {
    body = (
      <div className={styles.outputsList}>
        {load.outputs.map((out) => {
          const key = `${out.task_id}:${out.name}`
          return (
            <div key={key} className={styles.outputRow}>
              <span className={styles.taskChip}>{out.task_id}</span>
              <span className={styles.outputName}>{out.name}</span>
              <details>
                <pre className={styles.jsonBlock}>{pretty(out.value)}</pre>
              </details>
              <button type="button" className={styles.copyButton} onClick={() => { copy(key, out.value) }}>
                {tx(locale, copied === key ? 'action.copied' : 'action.copy')}
              </button>
            </div>
          )
        })}
      </div>
    )
  }

  return <div className={styles.centerPanel}>{body}</div>
}
