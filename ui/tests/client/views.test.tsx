// @vitest-environment jsdom
/**
 * Client view tests (jsdom): RunsListView envelope arms and DagGraphView
 * SVG node/edge rendering. No @testing-library — createRoot + act.
 */
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ApiResult } from '../../src/client/api.ts'
import { DagGraphView, type RunView } from '../../src/client/views/DagGraphView.tsx'
import { RunsListView } from '../../src/client/views/RunsListView.tsx'
import type { RunCounts, RunSummary, RunsView, SpecTask, TaskKind } from '../../src/core/types.ts'

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

function specTask(id: string, kind: TaskKind, dependsOn: string[] = []): SpecTask {
  return {
    id,
    kind,
    dependsOn: dependsOn.map((taskId) => ({ taskId, condition: 'succeeded' })),
  }
}

function byClass(container: ParentNode, name: string): Element | null {
  return container.querySelector(`.${name}`) ?? container.querySelector(`[class*="${name}"]`)
}

function allByClass(container: ParentNode, name: string): NodeListOf<Element> {
  const exact = container.querySelectorAll(`.${name}`)
  if (exact.length > 0) return exact
  return container.querySelectorAll(`[class*="${name}"]`)
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

describe('RunsListView', () => {
  it('renders two run rows and click calls onOpenRun with run_id', async () => {
    const runs: RunSummary[] = [
      runSummary({ run_id: 'run_aaaa1111', name: 'alpha-run', state: 'running' }),
      runSummary({ run_id: 'run_bbbb2222', name: 'beta-run', state: 'succeeded' }),
    ]
    const api = {
      runs: vi.fn(async (): Promise<ApiResult<RunsView>> => ({ ok: true, value: { runs } })),
    }
    const onOpenRun = vi.fn()
    const container = await render(
      <RunsListView api={api} locale="en" onOpenRun={onOpenRun} />,
    )
    await vi.waitFor(() => {
      expect(allByClass(container, 'runsRow')).toHaveLength(2)
    })
    expect(container.textContent).toContain('alpha-run')
    expect(container.textContent).toContain('beta-run')
    expect(container.textContent).toContain('run_aaaa')
    const row = allByClass(container, 'runsRow')[0]!
    await act(async () => {
      row.dispatchEvent(new MouseEvent('click', { bubbles: true }))
    })
    expect(onOpenRun).toHaveBeenCalledTimes(1)
    expect(onOpenRun).toHaveBeenCalledWith('run_aaaa1111')
  })

  it('shows errorState with the envelope code', async () => {
    const api = {
      runs: async (): Promise<ApiResult<RunsView>> => ({
        ok: false,
        error: { code: 'boom', message: 'x' },
      }),
    }
    const container = await render(
      <RunsListView api={api} locale="en" onOpenRun={() => { }} />,
    )
    await vi.waitFor(() => {
      const error = byClass(container, 'errorState')
      expect(error).not.toBeNull()
      expect(error!.textContent).toContain('boom')
    })
  })

  it('shows emptyState when the run list is empty', async () => {
    const api = {
      runs: async (): Promise<ApiResult<RunsView>> => ({ ok: true, value: { runs: [] } }),
    }
    const container = await render(
      <RunsListView api={api} locale="en" onOpenRun={() => { }} />,
    )
    await vi.waitFor(() => {
      expect(byClass(container, 'emptyState')).not.toBeNull()
    })
  })
})

describe('DagGraphView', () => {
  const fixture: RunView = {
    run_id: 'run_graph01',
    name: 'chain-3',
    state: 'running',
    counts: { ...ZERO_COUNTS, running: 1, succeeded: 1, pending: 1 },
    created_at: 1_000,
    updated_at: 2_000,
    spec: {
      version: 1,
      name: 'chain-3',
      tasks: [
        specTask('alpha', 'agent'),
        specTask('bravo', 'approval', ['alpha']),
        specTask('charlie', 'merge', ['bravo']),
      ],
    },
    tasks: [
      { id: 'alpha', state: 'succeeded', attempts: 1, ordinal: 0 },
      { id: 'bravo', state: 'running', attempts: 1, ordinal: 1 },
      { id: 'charlie', state: 'pending', attempts: 0, ordinal: 2 },
    ],
  }

  it('draws a shape per node, an edgeLine per edge, and click selects a node', async () => {
    const onSelectTask = vi.fn()
    const container = await render(
      <DagGraphView
        run={fixture}
        locale="en"
        onSelectTask={onSelectTask}
        onRefresh={() => { }}
      />,
    )
    const svg = container.querySelector('svg')
    expect(svg).not.toBeNull()
    const nodeCount = fixture.spec.tasks.length
    expect(svg!.querySelectorAll('rect').length).toBeGreaterThanOrEqual(nodeCount)
    const edgeLines = [...svg!.querySelectorAll('polyline, path')].filter((el) => {
      const cls = el.getAttribute('class') ?? ''
      return cls === 'edgeLine' || cls.includes('edgeLine')
    })
    expect(edgeLines).toHaveLength(2)
    const group = container.querySelector('[data-node-id="alpha"]') ?? svg!.querySelector('g')
    expect(group).not.toBeNull()
    await act(async () => {
      group!.dispatchEvent(new MouseEvent('click', { bubbles: true }))
    })
    expect(onSelectTask).toHaveBeenCalledWith('alpha')
  })
})
