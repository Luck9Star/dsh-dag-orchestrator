/**
 * Route-layer tests: fake ctx (webServer.register spy capturing the
 * handler) + a canned fake dagOrchestrator face (shapes from
 * dsh-dag-orchestrator lib/dag-face.js), driven through minimal fake
 * {method, headers, url, body-stream} request objects.
 */
import { describe, expect, it, vi } from 'vitest'
import { apply, inject } from '../../src/index.ts'
import type { StatusAttempts, StatusEvents, StatusSummary } from '../../src/host/face.ts'

/** One fake request: method/headers/url plus a JSON body. */
function fakeRequest(method: string, url: string, body?: unknown, contentType = 'application/json'): {
  method: string
  url: string
  headers: Record<string, string>
  [Symbol.asyncIterator](): AsyncIterableIterator<Buffer>
} {
  const text = body === undefined ? '' : JSON.stringify(body)
  const buffer = Buffer.from(text, 'utf8')
  return {
    method,
    url,
    headers: { 'content-type': contentType },
    async *[Symbol.asyncIterator]() {
      if (buffer.length > 0) yield buffer
    },
  }
}

/** One fake response capturing writeHead/end calls into a state object. */
function fakeResponse(): {
  state: { status: number | undefined; headers: Record<string, string> | undefined; body: string }
  res: { writeHead(status: number, headers?: Record<string, string>): void; end(chunk?: string): void }
} {
  const state = { status: undefined as number | undefined, headers: undefined as Record<string, string> | undefined, body: '' }
  return {
    state,
    res: {
      writeHead(status: number, headers?: Record<string, string>) {
        state.status = status
        state.headers = headers
      },
      end(chunk?: string) {
        if (chunk !== undefined) state.body += chunk
      },
    },
  }
}

/** The canned fake dagOrchestrator face (dag-face.js shapes, frozen like the real one). */
function fakeFace() {
  const runs = [{
    run_id: 'dag_20260101_abcd1234',
    name: 'build-and-verify',
    state: 'running',
    counts: { pending: 1, ready: 1, running: 1, succeeded: 1, failed: 0, blocked: 0 },
    created_at: 1767225600000,
    updated_at: 1767225700000,
  }]
  const tasks = [
    { id: 'build', state: 'succeeded', attempts: 1, ordinal: 1 },
    { id: 'verify', state: 'running', attempts: 1, ordinal: 1 },
  ]
  const attempts = [
    { attempt_id: 'att_build_1', task_id: 'build', ordinal: 1, state: 'succeeded', backend: 'spawn', started_at: 1767225601000, child_session: 'sess_build_1', stop_reason: 'completed' },
    { attempt_id: 'att_verify_1', task_id: 'verify', ordinal: 1, state: 'running', backend: 'spawn', started_at: 1767225650000, child_session: 'sess_verify_1' },
  ]
  const events = [
    { seq: 1, type: 'run.created', at: 1767225600000, payload: { name: 'build-and-verify' } },
    { seq: 2, type: 'task.succeeded', at: 1767225605000, task_id: 'build', attempt_id: 'att_build_1', payload: { from: 'running', to: 'succeeded' } },
  ]
  const summaries = [
    { attempt_id: 'att_build_1', task_id: 'build', ordinal: 1, state: 'succeeded', backend: 'spawn', started_at: 1767225601000, child_session: 'sess_build_1', stop_reason: 'completed', summary: { done: true } },
    { attempt_id: 'att_verify_1', task_id: 'verify', ordinal: 1, state: 'running', backend: 'spawn', started_at: 1767225650000, child_session: 'sess_verify_1' },
  ]
  return Object.freeze({
    status: vi.fn((runId?: string | null, options?: { detail?: string; afterSeq?: number; taskId?: string; limit?: number }) => {
      if (runId === undefined || runId === null || runId === '') {
        return { kind: 'status', detail: 'summary', runs }
      }
      const base = {
        kind: 'status',
        run_id: runId,
        name: 'build-and-verify',
        state: 'running',
        counts: runs[0].counts,
        created_at: runs[0].created_at,
        updated_at: runs[0].updated_at,
      }
      if (options?.detail === 'events') {
        const filtered = events
          .filter((event) => options.afterSeq === undefined || event.seq > options.afterSeq)
          .filter((event) => options.taskId === undefined || event.task_id === options.taskId)
        return { ...base, detail: 'events', tasks, events: filtered }
      }
      return { ...base, detail: 'attempts', tasks, attempts }
    }),
    getSpec: vi.fn((runId: string) => {
      if (runId !== 'dag_20260101_abcd1234') {
        throw Object.assign(new Error('dag.run_not_found: run not found'), { code: 'dag.run_not_found' })
      }
      return {
        run_id: runId,
        name: 'build-and-verify',
        spec_hash: 'sha256:deadbeef',
        spec: {
          version: 1,
          name: 'build-and-verify',
          tasks: [
            { id: 'build', kind: 'agent', prompt: 'build it', dependsOn: [] },
            { id: 'verify', kind: 'agent', prompt: 'verify it', dependsOn: [{ taskId: 'build', condition: 'succeeded' }] },
          ],
        },
      }
    }),
    listOutputs: vi.fn(() => [
      { task_id: 'build', name: 'report', value: { ok: true }, produced_by_attempt: 'att_build_1' },
    ]),
    attemptSummaries: vi.fn(() => summaries),
  })
}

/** Build a fake ctx capturing the route registration. */
function fakeCtx(face?: unknown) {
  const registered: Array<{ kind: string; path: string; handler: (req: unknown, res: unknown) => Promise<void> }> = []
  const disposers: Array<() => void> = []
  const ctx = {
    webServer: {
      register: vi.fn((route: { kind: string; path: string; handler: (req: unknown, res: unknown) => Promise<void> }) => {
        registered.push(route)
        const dispose = (): void => { }
        disposers.push(dispose)
        return dispose
      }),
    },
    get: vi.fn((name: string) => (name === 'dagOrchestrator' ? face : undefined)),
    logger: { warn: vi.fn() },
    effect: vi.fn((callback: () => () => void, _label?: string) => callback()),
  }
  return { ctx, registered }
}

/** Invoke the captured handler with a fake request/response pair. */
async function call(handler: (req: unknown, res: unknown) => Promise<void>, method: string, url: string, body?: unknown, contentType?: string): Promise<{ status: number | undefined; body: string; json(): unknown }> {
  const response = fakeResponse()
  await handler(fakeRequest(method, url, body, contentType), response.res)
  return {
    status: response.state.status,
    body: response.state.body,
    json: () => JSON.parse(response.state.body),
  }
}

describe('host entry', () => {
  it('declares the webServer inject and registers one prefix route', () => {
    const { ctx, registered } = fakeCtx(fakeFace())
    apply(ctx as never)
    expect(inject).toEqual(['webServer'])
    expect(registered).toHaveLength(1)
    expect(registered[0]!.kind).toBe('prefix')
    expect(registered[0]!.path).toBe('/dag-view')
  })
})

describe('/dag-view routes', () => {
  it('rejects GET with 405', async () => {
    const { ctx, registered } = fakeCtx(fakeFace())
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'GET', '/dag-view/runs')
    expect(result.status).toBe(405)
    expect(result.body).toBe('')
  })

  it('rejects non-JSON content-type with 415', async () => {
    const { ctx, registered } = fakeCtx(fakeFace())
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/runs', {}, 'text/plain')
    expect(result.status).toBe(415)
    expect(result.body).toBe('')
  })

  it('returns the unavailable envelope when the face is absent', async () => {
    const { ctx, registered } = fakeCtx(undefined)
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/runs', {})
    expect(result.status).toBe(200)
    expect(result.json()).toEqual({
      ok: false,
      error: { code: 'dag_view.unavailable', message: 'dsh-dag-orchestrator not loaded' },
    })
  })

  it('answers /runs with the summary rows', async () => {
    const face = fakeFace()
    const { ctx, registered } = fakeCtx(face)
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/runs', {})
    expect(result.status).toBe(200)
    const envelope = result.json() as { ok: boolean; value?: { runs: StatusSummary['runs'] } }
    expect(envelope.ok).toBe(true)
    expect(envelope.value!.runs).toHaveLength(1)
    expect(envelope.value!.runs[0]!.run_id).toBe('dag_20260101_abcd1234')
    expect(face.status).toHaveBeenCalledWith(undefined, { detail: 'summary' })
  })

  it('answers /run with the aggregate snapshot', async () => {
    const face = fakeFace()
    const { ctx, registered } = fakeCtx(face)
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/run', { run_id: 'dag_20260101_abcd1234' })
    expect(result.status).toBe(200)
    const envelope = result.json() as {
      ok: boolean
      value?: {
        run: Record<string, unknown>
        spec: { tasks: Array<{ id: string }> }
        tasks: Array<{ id: string }>
        attempts: Array<{ attempt_id: string }>
        outputs: Array<{ task_id: string }>
      }
    }
    expect(envelope.ok).toBe(true)
    expect(envelope.value!.run['run_id']).toBe('dag_20260101_abcd1234')
    expect(envelope.value!.run['state']).toBe('running')
    expect(envelope.value!.run['counts']).toEqual({ pending: 1, ready: 1, running: 1, succeeded: 1, failed: 0, blocked: 0 })
    // kind/detail peeled off the run base; tasks/attempts split out.
    expect(envelope.value!.run['kind']).toBeUndefined()
    expect(envelope.value!.run['detail']).toBeUndefined()
    expect(envelope.value!.run['tasks']).toBeUndefined()
    expect(envelope.value!.run['attempts']).toBeUndefined()
    expect(envelope.value!.spec.tasks.map((task) => task.id)).toEqual(['build', 'verify'])
    expect(envelope.value!.tasks.map((task) => task.id)).toEqual(['build', 'verify'])
    expect(envelope.value!.attempts.map((attempt) => attempt.attempt_id)).toEqual(['att_build_1', 'att_verify_1'])
    expect(envelope.value!.outputs).toEqual([{ task_id: 'build', name: 'report', value: { ok: true }, produced_by_attempt: 'att_build_1' }])
    expect(face.getSpec).toHaveBeenCalledWith('dag_20260101_abcd1234')
    expect(face.status).toHaveBeenCalledWith('dag_20260101_abcd1234', { detail: 'attempts' })
    expect(face.listOutputs).toHaveBeenCalledWith('dag_20260101_abcd1234')
  })

  it('answers /events with the tail window and passes the filters', async () => {
    const face = fakeFace()
    const { ctx, registered } = fakeCtx(face)
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/events', { run_id: 'dag_20260101_abcd1234', after_seq: 1, task_id: 'build', limit: 10 })
    expect(result.status).toBe(200)
    const envelope = result.json() as { ok: boolean; value?: { events: StatusEvents['events'] } }
    expect(envelope.ok).toBe(true)
    expect(envelope.value!.events).toHaveLength(1)
    expect(envelope.value!.events[0]!.seq).toBe(2)
    expect(face.status).toHaveBeenCalledWith('dag_20260101_abcd1234', { detail: 'events', afterSeq: 1, taskId: 'build', limit: 10 })
  })

  it('answers /attempt-logs with the summaries', async () => {
    const face = fakeFace()
    const { ctx, registered } = fakeCtx(face)
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/attempt-logs', { run_id: 'dag_20260101_abcd1234' })
    expect(result.status).toBe(200)
    const envelope = result.json() as { ok: boolean; value?: { items: Array<{ attempt_id: string }> } }
    expect(envelope.ok).toBe(true)
    expect(envelope.value!.items.map((item) => item.attempt_id)).toEqual(['att_build_1', 'att_verify_1'])
    expect(face.attemptSummaries).toHaveBeenCalledWith('dag_20260101_abcd1234')
  })

  it('rejects a missing run_id with dag_view.bad_request', async () => {
    const { ctx, registered } = fakeCtx(fakeFace())
    apply(ctx as never)
    for (const path of ['/dag-view/run', '/dag-view/events', '/dag-view/attempt-logs']) {
      const result = await call(registered[0]!.handler, 'POST', path, {})
      expect(result.status).toBe(200)
      expect(result.json()).toEqual({
        ok: false,
        error: { code: 'dag_view.bad_request', message: 'missing run_id' },
      })
    }
  })

  it('rejects a malformed JSON body with dag_view.bad_request', async () => {
    const { ctx, registered } = fakeCtx(fakeFace())
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/runs', undefined)
    // undefined body serializes to an empty stream -> unparseable -> bad_request
    expect(result.status).toBe(200)
    expect(result.json()).toEqual({
      ok: false,
      error: { code: 'dag_view.bad_request', message: 'malformed JSON body' },
    })
  })

  it('passes dag.* face errors through as envelope codes', async () => {
    const face = fakeFace()
    face.getSpec.mockRejectedValueOnce(Object.assign(new Error('dag.run_not_found: run not found'), { code: 'dag.run_not_found' }))
    const { ctx, registered } = fakeCtx(face)
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/run', { run_id: 'dag_missing' })
    expect(result.status).toBe(200)
    expect(result.json()).toEqual({
      ok: false,
      error: { code: 'dag.run_not_found', message: 'dag.run_not_found: run not found' },
    })
  })

  it('maps unknown face errors to dag_view.internal with a logger.warn', async () => {
    const face = fakeFace()
    face.status.mockImplementationOnce(() => { throw new Error('sqlite exploded') })
    const { ctx, registered } = fakeCtx(face)
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/runs', {})
    expect(result.status).toBe(200)
    const envelope = result.json() as { ok: boolean; error?: { code: string } }
    expect(envelope.ok).toBe(false)
    expect(envelope.error!.code).toBe('dag_view.internal')
    expect(ctx.logger.warn).toHaveBeenCalled()
  })

  it('answers 404 for unknown paths under the prefix', async () => {
    const { ctx, registered } = fakeCtx(fakeFace())
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/nope', {})
    expect(result.status).toBe(404)
  })

  it('reports the unavailable envelope when ctx.get throws', async () => {
    const ctx = {
      webServer: { register: vi.fn() },
      get: () => { throw new Error('no such service') },
      logger: { warn: vi.fn() },
      effect: vi.fn((callback: () => () => void) => callback()),
    }
    const registered: Array<{ kind: string; path: string; handler: (req: unknown, res: unknown) => Promise<void> }> = []
    ctx.webServer.register.mockImplementation((route: { kind: string; path: string; handler: (req: unknown, res: unknown) => Promise<void> }) => {
      registered.push(route)
      return () => { }
    })
    apply(ctx as never)
    const result = await call(registered[0]!.handler, 'POST', '/dag-view/runs', {})
    expect(result.status).toBe(200)
    expect(result.json()).toEqual({
      ok: false,
      error: { code: 'dag_view.unavailable', message: 'dsh-dag-orchestrator not loaded' },
    })
  })
})
