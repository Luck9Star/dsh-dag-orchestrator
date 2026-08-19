/**
 * api.ts tests (jsdom): envelope decode for ok/error arms and the
 * transport-error arm, with fetch mocked at the global level.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { DagViewApi } from '../../src/client/api.ts'
import type { AttemptLogsView, RunAggregate, RunsView } from '../../src/core/types.ts'

/** Install a fetch mock resolving with the given envelope. */
function mockFetch(handler: (path: string, init?: RequestInit) => Promise<{ status: number; body: unknown }>): void {
  vi.stubGlobal('fetch', vi.fn(async (path: string | URL, init?: RequestInit) => {
    const result = await handler(typeof path === 'string' ? path : path.toString(), init)
    return {
      ok: result.status >= 200 && result.status < 300,
      status: result.status,
      json: async () => result.body,
    } as Response
  }))
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('DagViewApi envelope decode', () => {
  it('decodes the ok arm', async () => {
    mockFetch(async () => ({
      status: 200,
      body: { ok: true, value: { runs: [{ run_id: 'dag_x', name: 'n', state: 'running', counts: { pending: 0, ready: 0, running: 1, succeeded: 0, failed: 0, blocked: 0 }, created_at: 1, updated_at: 2 }] } },
    }))
    const api = new DagViewApi()
    const result = await api.runs()
    expect(result.ok).toBe(true)
    expect((result as { value: RunsView }).value.runs).toHaveLength(1)
    expect((result as { value: RunsView }).value.runs[0]!.run_id).toBe('dag_x')
  })

  it('POSTs /dag-view/runs with an empty JSON body', async () => {
    mockFetch(async () => ({ status: 200, body: { ok: true, value: { runs: [] } } }))
    const api = new DagViewApi()
    await api.runs()
    expect(fetch).toHaveBeenCalledWith('/dag-view/runs', expect.objectContaining({
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({}),
    }))
  })

  it('decodes the error arm with the envelope code', async () => {
    mockFetch(async () => ({
      status: 200,
      body: { ok: false, error: { code: 'dag_view.unavailable', message: 'dsh-dag-orchestrator not loaded' } },
    }))
    const api = new DagViewApi()
    const result = await api.runs()
    expect(result).toEqual({
      ok: false,
      error: { code: 'dag_view.unavailable', message: 'dsh-dag-orchestrator not loaded' },
    })
  })

  it('returns the network error when fetch rejects', async () => {
    const reason = new TypeError('network down')
    vi.stubGlobal('fetch', vi.fn(async () => { throw reason }))
    const api = new DagViewApi()
    const result = await api.runs()
    expect(result).toEqual({
      ok: false,
      error: { code: 'network', message: String(reason) },
    })
  })

  it('returns the transport error when the response is not an envelope', async () => {
    mockFetch(async () => ({ status: 200, body: 'plain text' }))
    const api = new DagViewApi()
    const result = await api.runs()
    expect(result.ok).toBe(false)
    expect((result as { error: { code: string } }).error.code).toBe('dag_view.transport')
  })

  it('returns the transport error when the error shape is malformed', async () => {
    mockFetch(async () => ({ status: 200, body: { ok: false, error: 'nope' } }))
    const api = new DagViewApi()
    const result = await api.runs()
    expect(result.ok).toBe(false)
    expect((result as { error: { code: string } }).error.code).toBe('dag_view.transport')
  })

  it('POSTs the run_id and decodes the aggregate', async () => {
    let capturedPath = ''
    let capturedBody = ''
    mockFetch(async (path, init) => {
      capturedPath = path
      capturedBody = String(init?.body ?? '')
      return {
        status: 200,
        body: {
          ok: true,
          value: {
            run: { run_id: 'dag_x', name: 'n', state: 'running', counts: { pending: 0, ready: 0, running: 1, succeeded: 0, failed: 0, blocked: 0 }, created_at: 1, updated_at: 2 },
            spec: { version: 1, name: 'n', tasks: [] },
            tasks: [],
            attempts: [],
            outputs: [],
          },
        },
      }
    })
    const api = new DagViewApi()
    const result = await api.run('dag_x')
    expect(capturedPath).toBe('/dag-view/run')
    expect(JSON.parse(capturedBody)).toEqual({ run_id: 'dag_x' })
    expect(result.ok).toBe(true)
    expect((result as { value: RunAggregate }).value.run.run_id).toBe('dag_x')
  })

  it('serializes the events filters into the payload', async () => {
    mockFetch(async () => ({ status: 200, body: { ok: true, value: { events: [] } } }))
    const api = new DagViewApi()
    const result = await api.events('dag_x', { after_seq: 5, task_id: 'build', limit: 20 })
    expect(result.ok).toBe(true)
    expect(fetch).toHaveBeenCalledWith('/dag-view/events', expect.objectContaining({
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ run_id: 'dag_x', after_seq: 5, task_id: 'build', limit: 20 }),
    }))
  })

  it('omits undefined optional events fields from the JSON body', async () => {
    mockFetch(async () => ({ status: 200, body: { ok: true, value: { events: [] } } }))
    const api = new DagViewApi()
    await api.events('dag_x')
    const emptyOpts = await api.events('dag_x', {})
    expect(emptyOpts.ok).toBe(true)
    const calls = vi.mocked(fetch).mock.calls
    expect(calls).toHaveLength(2)
    for (const [, init] of calls) {
      const body = JSON.parse(String((init as RequestInit | undefined)?.body ?? '')) as Record<string, unknown>
      expect(body).toEqual({ run_id: 'dag_x' })
      expect(Object.keys(body)).toEqual(['run_id'])
    }
    await api.events('dag_x', { limit: 10 })
    const partial = JSON.parse(String((vi.mocked(fetch).mock.calls[2]?.[1] as RequestInit | undefined)?.body ?? '')) as Record<string, unknown>
    expect(partial).toEqual({ run_id: 'dag_x', limit: 10 })
    expect(Object.prototype.hasOwnProperty.call(partial, 'after_seq')).toBe(false)
    expect(Object.prototype.hasOwnProperty.call(partial, 'task_id')).toBe(false)
  })

  it('POSTs attempt-logs run_id and decodes items', async () => {
    mockFetch(async () => ({
      status: 200,
      body: {
        ok: true,
        value: {
          items: [{
            attempt_id: 'att_1',
            task_id: 'build',
            ordinal: 1,
            state: 'succeeded',
            backend: 'spawn',
            started_at: 1,
            child_session: 'sess_1',
            stop_reason: 'completed',
            summary: { done: true },
          }],
        },
      },
    }))
    const api = new DagViewApi()
    const result = await api.attemptLogs('dag_x')
    expect(fetch).toHaveBeenCalledWith('/dag-view/attempt-logs', expect.objectContaining({
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ run_id: 'dag_x' }),
    }))
    expect(result.ok).toBe(true)
    expect((result as { value: AttemptLogsView }).value.items).toHaveLength(1)
    expect((result as { value: AttemptLogsView }).value.items[0]!.attempt_id).toBe('att_1')
    expect((result as { value: AttemptLogsView }).value.items[0]!.child_session).toBe('sess_1')
  })
})
