/**
 * Locale-aware display helpers for the dag-view surface. Pure functions —
 * no DOM, no Date formatting APIs that depend on the host locale.
 * @module dsh-dag-view/core/format
 */

export type UiLocale = 'zh' | 'en'

const MS = 1
const SEC = 1000 * MS
const MIN = 60 * SEC
const HOUR = 60 * MIN
const DAY = 24 * HOUR

/** Map a task/run/attempt state (and common aliases) to a CSS tone class. */
const TONE_BY_STATE: Readonly<Record<string, string>> = {
  pending: 'state-tone-pending',
  ready: 'state-tone-ready',
  running: 'state-tone-running',
  succeeded: 'state-tone-succeeded',
  failed: 'state-tone-failed',
  blocked: 'state-tone-blocked',
  cancelled: 'state-tone-cancelled',
  canceled: 'state-tone-cancelled',
  retry_wait: 'state-tone-retry-wait',
  retryWait: 'state-tone-retry-wait',
  'retry-wait': 'state-tone-retry-wait',
  // Extra TaskState / RunState / AttemptState spellings from types.ts
  queued: 'state-tone-queued',
  pausing: 'state-tone-pausing',
  paused: 'state-tone-paused',
  cancelling: 'state-tone-cancelling',
  canceling: 'state-tone-cancelling',
  claimed: 'state-tone-claimed',
  orphaned: 'state-tone-orphaned',
}

const COUNT_ORDER = ['succeeded', 'failed', 'blocked', 'running', 'pending', 'cancelled'] as const

const COUNT_LABEL: Record<UiLocale, Record<(typeof COUNT_ORDER)[number], string>> = {
  en: {
    succeeded: 'ok',
    failed: 'failed',
    blocked: 'blocked',
    running: 'running',
    pending: 'pending',
    cancelled: 'cancelled',
  },
  zh: {
    succeeded: '成功',
    failed: '失败',
    blocked: '阻塞',
    running: '运行中',
    pending: '等待',
    cancelled: '已取消',
  },
}

export interface FormatCountsInput {
  readonly succeeded?: number
  readonly failed?: number
  readonly blocked?: number
  readonly running?: number
  readonly pending?: number
  readonly cancelled?: number
}

/**
 * Compact relative time. `now`/`then` are millisecond timestamps.
 * Non-positive diffs (future or equal) collapse to "just now".
 */
export function relativeTime(now: number, then: number, locale: UiLocale): string {
  const diff = now - then
  if (!Number.isFinite(diff) || diff <= 0) {
    return locale === 'zh' ? '刚刚' : 'just now'
  }
  if (diff < MIN) {
    const seconds = Math.floor(diff / SEC)
    if (seconds < 1) return locale === 'zh' ? '刚刚' : 'just now'
    return locale === 'zh' ? `${seconds}秒前` : `${seconds}s ago`
  }
  if (diff < HOUR) {
    const minutes = Math.floor(diff / MIN)
    return locale === 'zh' ? `${minutes}分钟前` : `${minutes}m ago`
  }
  if (diff < DAY) {
    const hours = Math.floor(diff / HOUR)
    return locale === 'zh' ? `${hours}小时前` : `${hours}h ago`
  }
  const days = Math.floor(diff / DAY)
  return locale === 'zh' ? `${days}天前` : `${days}d ago`
}

/**
 * Human duration from a millisecond span. Sub-second stays in ms; sub-minute
 * uses seconds (one decimal when needed); larger spans emit `h`/`m`/`s`
 * parts, omitting leading zero units.
 */
export function formatDuration(ms: number): string {
  const span = Number.isFinite(ms) && ms > 0 ? ms : 0
  if (span < SEC) return `${Math.round(span)}ms`
  if (span < MIN) {
    const rounded = Math.round((span / SEC) * 10) / 10
    return `${rounded}s`
  }
  const totalSec = Math.floor(span / SEC)
  const hours = Math.floor(totalSec / 3600)
  const minutes = Math.floor((totalSec % 3600) / 60)
  const seconds = totalSec % 60
  const parts: string[] = []
  if (hours > 0) parts.push(`${hours}h`)
  if (minutes > 0) parts.push(`${minutes}m`)
  if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`)
  return parts.join(' ')
}

/** CSS tone class for a lifecycle state. Unknown values default to pending. */
export function stateTone(state: string): string {
  return TONE_BY_STATE[state] ?? 'state-tone-pending'
}

/**
 * Compact count summary. Only non-zero buckets are emitted, in
 * succeeded → failed → blocked → running → pending → cancelled order.
 * Empty / all-zero / missing input → `"0 ok"` / `"0 成功"`.
 */
export function formatCounts(
  counts: FormatCountsInput | undefined | null,
  locale: UiLocale,
): string {
  const labels = COUNT_LABEL[locale]
  if (counts == null) return `0 ${labels.succeeded}`
  const segments: string[] = []
  for (const key of COUNT_ORDER) {
    const value = counts[key]
    if (typeof value === 'number' && Number.isFinite(value) && value !== 0) {
      segments.push(`${value} ${labels[key]}`)
    }
  }
  return segments.length === 0 ? `0 ${labels.succeeded}` : segments.join(' / ')
}
