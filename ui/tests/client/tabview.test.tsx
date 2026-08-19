// @vitest-environment jsdom
/**
 * DagTabView tests (jsdom): the conversation-tab body renders the
 * session-linked section first, the all-runs section below, the guidance
 * card when no runs exist at all, and hands an opened run to the embedded
 * DagViewApp tree. No @testing-library — createRoot + act.
 */
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ApiResult } from '../../src/client/api.ts'
import type { DagTabContext } from '../../src/client/views/DagTabView.tsx'
import { DagTabView } from '../../src/client/views/DagTabView.tsx'
import type { RunCounts, RunSummary, RunsView, SessionRunsView } from '../../src/core/types.ts'

;(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true

const ZERO_COUNTS: RunCounts = {
  pending: 0,
  ready: 0,
  running: 0,
  succeeded: 0,
  failed: 0,
  blocked: 0,
}

function runSummary(over: Partial<RunSummary> & Pick<RunSummary, 'run_id' | 'name'>): RunSummary {
  return {
    state: 'running',
    counts: ZERO_COUNTS,
    created_at: 1_000,
    updated_at: 2_000,
    ...over,
  }
}

/** Fake api with both list endpoints as spies (events/attemptLogs return envelopes too, so the embedded run view stays quiet). */
function fakeApi({ sessionRuns = [], allRuns = [] }: { sessionRuns?: RunSummary[]; allRuns?: RunSummary[] } = {}) {
  return {
    runs: vi.fn(async (): Promise<ApiResult<RunsView>> => ({ ok: true, value: { runs: allRuns } })),
    sessionRuns: vi.fn(async (sessionId: string): Promise<ApiResult<SessionRunsView>> => {
      expect(typeof sessionId).toBe('string')
      return { ok: true, value: { runs: sessionRuns } }
    }),
    run: vi.fn(),
    events: vi.fn(async (): Promise<ApiResult<{ events: never[] }>> => ({ ok: true, value: { events: [] } })),
    attemptLogs: vi.fn(async (): Promise<ApiResult<{ items: never[] }>> => ({ ok: true, value: { items: [] } })),
  }
}

/** The injected mount context (index.ts builds the real one). currentSession omitted = no session. */
function fakeDag(api: ReturnType<typeof fakeApi>, currentSession?: string): DagTabContext {
  return {
    getSessionId: () => currentSession,
    api: api as never,
    locale: 'en',
  }
}

function byText(container: ParentNode, text: string): Element | null {
  return [...container.querySelectorAll('div, span, p, button')].find((el) => el.textContent === text) ?? null
}

/** All run cards, in document order (css modules are inert in vitest — data attributes are the stable hook). */
function runCards(container: ParentNode): Element[] {
  return [...container.querySelectorAll('[data-run-id]')]
}

let root: Root | undefined
let host: HTMLDivElement | undefined

async function render(node: JSX.Element): Promise<HTMLDivElement> {
  host = document.createElement('div')
  document.body.appendChild(host)
  root = createRoot(host)
  await act(async () => {
    root!.render(node)
  })
  return host
}

afterEach(() => {
  act(() => {
    root?.unmount()
  })
  host?.remove()
  root = undefined
  host = undefined
  document.body.innerHTML = ''
})

describe('DagTabView', () => {
  it('renders the session section first, then all runs, and session-runs is fetched with the session id', async () => {
    const sessionRuns = [runSummary({ run_id: 'dag_mine1', name: 'mine-run', state: 'succeeded' })]
    const allRuns = [
      runSummary({ run_id: 'dag_mine1', name: 'mine-run', state: 'succeeded' }),
      runSummary({ run_id: 'dag_other', name: 'other-run', state: 'running' }),
    ]
    const api = fakeApi({ sessionRuns, allRuns })
    const container = await render(<DagTabView dag={fakeDag(api)} sessionId="sess_gui_1" />)

    await vi.waitFor(() => {
      expect(runCards(container)).toHaveLength(3)
    })
    expect(api.sessionRuns).toHaveBeenCalledWith('sess_gui_1')
    expect(byText(container, 'Runs in this conversation')).not.toBeNull()
    expect(byText(container, 'All runs')).not.toBeNull()
    // Session card renders before the all-runs cards (document order).
    const cards = runCards(container)
    expect(cards[0]!.textContent).toContain('mine-run')
    expect(cards[2]!.textContent).toContain('other-run')
  })

  it('shows the no-session-runs note when the session has none but runs exist elsewhere', async () => {
    const allRuns = [runSummary({ run_id: 'dag_other', name: 'other-run' })]
    const api = fakeApi({ allRuns })
    const container = await render(<DagTabView dag={fakeDag(api)} sessionId="sess_gui_1" />)

    await vi.waitFor(() => {
      expect(runCards(container)).toHaveLength(1)
    })
    expect(byText(container, 'No DAG runs in this conversation yet')).not.toBeNull()
    expect(byText(container, 'All runs')).not.toBeNull()
  })

  it('shows the centered guidance card when there are no runs at all', async () => {
    const api = fakeApi()
    const container = await render(<DagTabView dag={fakeDag(api)} sessionId="sess_gui_1" />)

    await vi.waitFor(() => {
      expect(container.querySelectorAll('[data-dag-empty-card]')).toHaveLength(1)
    })
    expect(byText(container, 'No DAG runs yet')).not.toBeNull()
    expect(api.runs).toHaveBeenCalled()
  })

  it('renders the run view inline when a card is clicked (DagViewApp embed)', async () => {
    const allRuns = [runSummary({ run_id: 'dag_open01', name: 'openable' })]
    const api = fakeApi({ allRuns })
    api.run = vi.fn(async (): Promise<ApiResult<never>> => ({
      ok: false,
      error: { code: 'dag.run_not_found', message: 'dag.run_not_found: gone' },
    })) as never
    const container = await render(<DagTabView dag={fakeDag(api)} sessionId="sess_gui_1" />)

    await vi.waitFor(() => {
      expect(runCards(container)).toHaveLength(1)
    })
    const card = runCards(container)[0]!
    await act(async () => {
      card.dispatchEvent(new MouseEvent('click', { bubbles: true }))
    })
    // The embedded DagViewApp surfaced the run-scoped fetch (its error arm).
    await vi.waitFor(() => {
      expect(api.run).toHaveBeenCalledWith('dag_open01')
    })
  })

  it('degrades to the all-runs section when session-runs errors', async () => {
    const allRuns = [runSummary({ run_id: 'dag_any1', name: 'any-run' })]
    const api = fakeApi({ allRuns })
    api.sessionRuns = vi.fn(async (): Promise<ApiResult<SessionRunsView>> => ({
      ok: false,
      error: { code: 'dag_view.unavailable', message: 'x' },
    }))
    const container = await render(<DagTabView dag={fakeDag(api)} sessionId="sess_gui_1" />)

    await vi.waitFor(() => {
      expect(runCards(container)).toHaveLength(1)
    })
    expect(byText(container, 'All runs')).not.toBeNull()
    // No crash and no error state: the session rows are an enhancement.
    expect(container.textContent).not.toContain('dag_view.unavailable')
  })

  it('skips the session call when no session is resolvable', async () => {
    const allRuns = [runSummary({ run_id: 'dag_solo1', name: 'solo' })]
    const api = fakeApi({ allRuns })
    const container = await render(<DagTabView dag={fakeDag(api, undefined)} />)

    await vi.waitFor(() => {
      expect(runCards(container)).toHaveLength(1)
    })
    expect(api.sessionRuns).not.toHaveBeenCalled()
    expect(api.runs).toHaveBeenCalled()
    // Without a current session there is no session-section note either.
    expect(byText(container, 'No DAG runs in this conversation yet')).toBeNull()
  })
})
