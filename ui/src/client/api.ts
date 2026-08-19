/**
 * Browser client for the host /dag-view/* routes: typed JSON envelope
 * calls over same-origin fetch (the page and the routes share the
 * webserver). Never throws — every failure is an error arm of ApiResult.
 * @module dsh-dag-view/client/api
 */

import type {
  AttemptLogsView, EventsView, RunAggregate, RunsView,
} from '../core/types.ts'

/** One /dag-view envelope response. */
export type ApiResult<T> =
  | { ok: true; value: T }
  | { ok: false; error: { code: string; message: string } }

/** Optional filters for POST /dag-view/events (undefined fields are omitted from the body). */
export type EventsOptions = {
  after_seq?: number
  task_id?: string
  limit?: number
}

/** Decode failure (the response was not a JSON envelope). */
const TRANSPORT_ERROR = { code: 'dag_view.transport', message: 'dag-view route unavailable' } as const

/** POST one JSON payload and decode the envelope; never throws. */
async function post<T>(path: string, payload: Record<string, unknown>): Promise<ApiResult<T>> {
  let response: Response
  try {
    response = await fetch(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    })
  } catch (reason) {
    return { ok: false, error: { code: 'network', message: String(reason) } }
  }
  try {
    const envelope = await response.json() as unknown
    if (typeof envelope !== 'object' || envelope === null) return { ok: false, error: TRANSPORT_ERROR }
    const record = envelope as Record<string, unknown>
    if (record.ok === true) return { ok: true, value: record.value as T }
    const error = record.error
    if (typeof error === 'object' && error !== null) {
      const fields = error as Record<string, unknown>
      if (typeof fields.code === 'string' && typeof fields.message === 'string') {
        return { ok: false, error: { code: fields.code, message: fields.message } }
      }
    }
    return { ok: false, error: TRANSPORT_ERROR }
  } catch {
    return { ok: false, error: TRANSPORT_ERROR }
  }
}

/** Typed dag-view operations over the wire. */
export class DagViewApi {
  /** All runs (summary rows with state badges and task counts). */
  runs(): Promise<ApiResult<RunsView>> {
    return post('/dag-view/runs', {})
  }

  /** One aggregate snapshot for the run view (spec + tasks + attempts + outputs). */
  run(run_id: string): Promise<ApiResult<RunAggregate>> {
    return post('/dag-view/run', { run_id })
  }

  /** The event tail window, optionally incremental (after_seq) and filtered. */
  events(run_id: string, opts?: EventsOptions): Promise<ApiResult<EventsView>> {
    const payload: Record<string, unknown> = { run_id }
    if (opts?.after_seq !== undefined) payload.after_seq = opts.after_seq
    if (opts?.task_id !== undefined) payload.task_id = opts.task_id
    if (opts?.limit !== undefined) payload.limit = opts.limit
    return post('/dag-view/events', payload)
  }

  /** Attempt summaries of one run (child_session ids + result summaries). */
  attemptLogs(run_id: string): Promise<ApiResult<AttemptLogsView>> {
    return post('/dag-view/attempt-logs', { run_id })
  }
}
