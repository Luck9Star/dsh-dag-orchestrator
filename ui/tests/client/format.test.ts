/**
 * format.ts tests: relative time, duration, state tone totality, and
 * locale-aware count summaries. Pure functions — no DOM.
 */
import { describe, expect, it } from 'vitest'
import { formatCounts, formatDuration, relativeTime, stateTone } from '../../src/core/format.ts'
import type { AttemptState, RunState, TaskState } from '../../src/core/types.ts'

const TASK_STATES: readonly TaskState[] = [
  'pending',
  'ready',
  'queued',
  'running',
  'retry_wait',
  'blocked',
  'succeeded',
  'failed',
  'cancelled',
]

const RUN_STATES: readonly RunState[] = [
  'running',
  'pausing',
  'paused',
  'cancelling',
  'succeeded',
  'failed',
  'cancelled',
]

const ATTEMPT_STATES: readonly AttemptState[] = [
  'claimed',
  'running',
  'succeeded',
  'failed',
  'cancelled',
  'orphaned',
]

const CANONICAL = [
  'pending',
  'ready',
  'running',
  'succeeded',
  'failed',
  'blocked',
  'cancelled',
  'retry_wait',
] as const

const CANONICAL_TONES = [
  'state-tone-pending',
  'state-tone-ready',
  'state-tone-running',
  'state-tone-succeeded',
  'state-tone-failed',
  'state-tone-blocked',
  'state-tone-cancelled',
  'state-tone-retry-wait',
] as const

describe('relativeTime', () => {
  const now = 1_700_000_000_000

  it('returns just-now for zero and negative diffs (en + zh)', () => {
    expect(relativeTime(now, now, 'en')).toBe('just now')
    expect(relativeTime(now, now + 5_000, 'en')).toBe('just now')
    expect(relativeTime(now, now, 'zh')).toBe('刚刚')
    expect(relativeTime(now, now + 1, 'zh')).toBe('刚刚')
  })

  it('formats seconds in both locales', () => {
    expect(relativeTime(now, now - 45_000, 'en')).toBe('45s ago')
    expect(relativeTime(now, now - 45_000, 'zh')).toBe('45秒前')
    expect(relativeTime(now, now - 1_000, 'en')).toBe('1s ago')
    expect(relativeTime(now, now - 1_000, 'zh')).toBe('1秒前')
  })

  it('formats minutes in both locales', () => {
    expect(relativeTime(now, now - 3 * 60_000, 'en')).toBe('3m ago')
    expect(relativeTime(now, now - 3 * 60_000, 'zh')).toBe('3分钟前')
  })

  it('formats hours in both locales', () => {
    expect(relativeTime(now, now - 2 * 3_600_000, 'en')).toBe('2h ago')
    expect(relativeTime(now, now - 2 * 3_600_000, 'zh')).toBe('2小时前')
  })

  it('formats days in both locales', () => {
    expect(relativeTime(now, now - 1 * 86_400_000, 'en')).toBe('1d ago')
    expect(relativeTime(now, now - 1 * 86_400_000, 'zh')).toBe('1天前')
    expect(relativeTime(now, now - 3 * 86_400_000, 'en')).toBe('3d ago')
    expect(relativeTime(now, now - 3 * 86_400_000, 'zh')).toBe('3天前')
  })
})

describe('formatDuration', () => {
  it('renders sub-second spans as milliseconds', () => {
    expect(formatDuration(123)).toBe('123ms')
    expect(formatDuration(0)).toBe('0ms')
    expect(formatDuration(999)).toBe('999ms')
  })

  it('renders sub-minute spans as seconds (one decimal when needed)', () => {
    expect(formatDuration(1_200)).toBe('1.2s')
    expect(formatDuration(45_000)).toBe('45s')
    expect(formatDuration(1_000)).toBe('1s')
  })

  it('renders minutes plus leftover seconds', () => {
    expect(formatDuration(3 * 60_000 + 5_000)).toBe('3m 5s')
    expect(formatDuration(60_000)).toBe('1m')
  })

  it('renders hours, omitting leading zero units', () => {
    expect(formatDuration(1 * 3_600_000 + 2 * 60_000 + 3_000)).toBe('1h 2m 3s')
    expect(formatDuration(3_600_000)).toBe('1h')
    expect(formatDuration(3_600_000 + 3_000)).toBe('1h 3s')
  })
})

describe('stateTone', () => {
  it('maps every TaskState / RunState / AttemptState to a state-tone-* class', () => {
    const all = new Set<string>([...TASK_STATES, ...RUN_STATES, ...ATTEMPT_STATES])
    for (const state of all) {
      const tone = stateTone(state)
      expect(tone.length).toBeGreaterThan('state-tone-'.length)
      expect(tone.startsWith('state-tone-')).toBe(true)
    }
  })

  it('maps the 8 canonical states to 8 distinct classes', () => {
    const tones = CANONICAL.map((state) => stateTone(state))
    expect(tones).toEqual([...CANONICAL_TONES])
    expect(new Set(tones).size).toBe(8)
  })

  it('defaults unknown states to pending and accepts common aliases', () => {
    expect(stateTone('mystery')).toBe('state-tone-pending')
    expect(stateTone('')).toBe('state-tone-pending')
    expect(stateTone('retryWait')).toBe('state-tone-retry-wait')
    expect(stateTone('canceled')).toBe('state-tone-cancelled')
  })
})

describe('formatCounts', () => {
  it('joins non-zero buckets in priority order (en + zh)', () => {
    const mixed = { succeeded: 3, failed: 1, blocked: 1, running: 2, pending: 4, cancelled: 1 }
    expect(formatCounts(mixed, 'en')).toBe('3 ok / 1 failed / 1 blocked / 2 running / 4 pending / 1 cancelled')
    expect(formatCounts(mixed, 'zh')).toBe('3 成功 / 1 失败 / 1 阻塞 / 2 运行中 / 4 等待 / 1 已取消')
    expect(formatCounts({ succeeded: 3, failed: 1, blocked: 1 }, 'en')).toBe('3 ok / 1 failed / 1 blocked')
    expect(formatCounts({ succeeded: 3, failed: 1, blocked: 1 }, 'zh')).toBe('3 成功 / 1 失败 / 1 阻塞')
  })

  it('omits zero buckets and collapses all-zero / empty / missing to 0 ok', () => {
    expect(formatCounts({ succeeded: 0, failed: 0, blocked: 0 }, 'en')).toBe('0 ok')
    expect(formatCounts({ succeeded: 0, failed: 0, blocked: 0 }, 'zh')).toBe('0 成功')
    expect(formatCounts({}, 'en')).toBe('0 ok')
    expect(formatCounts(undefined, 'en')).toBe('0 ok')
    expect(formatCounts(undefined, 'zh')).toBe('0 成功')
    expect(formatCounts(null, 'en')).toBe('0 ok')
    expect(formatCounts(null, 'zh')).toBe('0 成功')
    expect(formatCounts({ succeeded: 3, failed: 0, blocked: 1 }, 'en')).toBe('3 ok / 1 blocked')
  })
})
