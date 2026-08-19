/**
 * Task detail: one task's status row, spec fields, and attempt list.
 * @module dsh-dag-view/client/views/TaskDetailView
 */

import { useState, type ReactNode } from 'react'
import { relativeTime, stateTone } from '../../core/format.ts'
import type { AttemptRow, SpecTask, TaskRow } from '../../core/types.ts'
import { getDict, type DagViewKey, type Locale } from '../locales.ts'
import styles from '../styles.module.css'

export interface TaskDetailViewProps {
  readonly run: any
  readonly taskId: string
  readonly locale: 'zh' | 'en'
  readonly onViewLog: (attemptOrdinal: number) => void
}

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

function kindLabel(locale: Locale, kind: string): string {
  const key = `kind.${kind}` as DagViewKey
  const dict = getDict(locale)
  return key in dict ? dict[key] : kind
}

function toneClass(state: string): string {
  const key = stateTone(state)
  return styles[key] ?? key
}

function asList<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : []
}

function blockedParts(reason: unknown): { code?: string; details?: unknown } {
  let value: unknown = reason
  if (typeof reason === 'string') {
    try { value = JSON.parse(reason) } catch { return { code: reason } }
  }
  if (typeof value !== 'object' || value === null) return {}
  const rec = value as Record<string, unknown>
  const { code, details, ...rest } = rec
  const extra = Object.keys(rest).length > 0 ? rest : undefined
  return {
    code: typeof code === 'string' ? code : undefined,
    details: details ?? extra,
  }
}

function Row({ locale, k, children }: { locale: Locale; k: DagViewKey; children?: ReactNode }): JSX.Element | null {
  if (children == null || children === '') return null
  return (
    <>
      <span className={styles.detailLabel}>{tx(locale, k)}</span>
      <span className={styles.detailValue}>{children}</span>
    </>
  )
}

/** Render one task from a run aggregate: status, spec, and attempts. */
export function TaskDetailView({
  run,
  taskId,
  locale,
  onViewLog,
}: TaskDetailViewProps): JSX.Element {
  const [copied, setCopied] = useState<string | undefined>(undefined)
  const task = asList<TaskRow>(run?.tasks).find((row) => row.id === taskId)
  const spec = asList<SpecTask>(run?.spec?.tasks).find((row) => row.id === taskId)
  const attempts = asList<AttemptRow>(run?.attempts)
    .filter((row) => row.task_id === taskId)
    .sort((a, b) => a.ordinal - b.ordinal)
  const inputs = asList<string>((spec as { inputs?: unknown } | undefined)?.inputs)

  if (task === undefined && spec === undefined) {
    return <div className={styles.emptyState}>{tx(locale, 'empty.taskDetail')}</div>
  }

  const state = task?.state ?? 'pending'
  const blocked = task?.blocked_reason !== undefined ? blockedParts(task.blocked_reason) : {}
  const now = Date.now()

  const copy = (text: string, key: string): void => {
    void navigator.clipboard.writeText(text).then(() => {
      setCopied(key)
      window.setTimeout(() => { setCopied((cur) => (cur === key ? undefined : cur)) }, 1200)
    })
  }

  return (
    <div className={styles.detailPanel}>
      <span className={`${styles.stateBadge} ${toneClass(state)}`}>{stateLabel(locale, state)}</span>
      <div className={styles.detailGrid}>
        <Row locale={locale} k="detail.ordinal">{task?.ordinal}</Row>
        <Row locale={locale} k="detail.attemptsCount">{task?.attempts ?? 0}</Row>
        <Row locale={locale} k="detail.blockedReason">
          {blocked.code}
          {blocked.details !== undefined ? (
            <pre className={styles.jsonBlock}>{JSON.stringify(blocked.details, null, 2)}</pre>
          ) : null}
        </Row>
        <Row locale={locale} k="detail.retryNotBefore">
          {task?.retry_not_before !== undefined
            ? relativeTime(now, task.retry_not_before, locale)
            : undefined}
        </Row>
        <Row locale={locale} k="detail.lastStopReason">{task?.last_stop_reason}</Row>
        <Row locale={locale} k="detail.kind">{spec ? kindLabel(locale, spec.kind) : undefined}</Row>
        <Row locale={locale} k="detail.model">{spec?.model}</Row>
        <Row locale={locale} k="detail.provider">{spec?.provider}</Row>
      </div>
      {spec?.prompt !== undefined ? (
        <details className={styles.promptDetails}>
          <summary>{tx(locale, 'detail.prompt')}</summary>
          <pre className={styles.preWrap}>{spec.prompt}</pre>
        </details>
      ) : null}
      {inputs.length > 0 ? (
        <div>
          <span className={styles.detailLabel}>{tx(locale, 'detail.inputs')}</span>
          <div>{inputs.map((ref) => <span key={ref} className={styles.chip}>{ref}</span>)}</div>
        </div>
      ) : null}
      <div className={styles.detailLabel}>{tx(locale, 'detail.attempts')}</div>
      {attempts.length === 0 ? (
        <div className={styles.emptyState}>{tx(locale, 'empty.noAttempts')}</div>
      ) : (
        <div className={styles.attemptsList}>
          {attempts.map((a) => {
            const copyText = a.child_session ?? a.attempt_id
            return (
              <div key={a.attempt_id} className={styles.attemptRow}>
                <span>{a.ordinal}</span>
                <span className={`${styles.stateBadge} ${toneClass(a.state)}`}>
                  {stateLabel(locale, a.state)}
                </span>
                <span className={styles.mono}>{a.backend}</span>
                <span className={styles.updatedAgo}>{relativeTime(now, a.started_at, locale)}</span>
                {a.stop_reason !== undefined ? <span>{a.stop_reason}</span> : null}
                {a.child_session !== undefined ? (
                  <span className={styles.mono}>{a.child_session.slice(0, 8)}</span>
                ) : null}
                <span className={styles.attemptActions}>
                  <button
                    type="button"
                    className={styles.backButton}
                    onClick={() => { onViewLog(a.ordinal) }}
                  >
                    {tx(locale, 'action.viewLog')}
                  </button>
                  <button
                    type="button"
                    className={styles.copyButton}
                    onClick={() => { copy(copyText, a.attempt_id) }}
                  >
                    {tx(locale, copied === a.attempt_id ? 'action.copied' : 'action.copy')}
                  </button>
                </span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
