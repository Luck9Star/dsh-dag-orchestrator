/**
 * /dag-view/* route layer: the read-only JSON data channel between the
 * browser half and the dsh-dag-orchestrator service face. POST-only with a
 * required application/json content-type (cross-site forms cannot send a
 * JSON content-type without a CORS preflight), a 1 MiB body cap (oversized
 * bodies are destroyed, never drained), and the shared envelope
 * `{ok:true,value}` | `{ok:false,error:{code,message}}` everywhere.
 *
 * The layer owns only HTTP shape; every fact comes from the
 * dagOrchestrator face resolved lazily per request (see face.ts).
 * @module dsh-dag-view/host/routes
 */

import type { IncomingMessage, ServerResponse } from 'node:http'
import type { Context } from '@deepseek-ai/cordis'
import type { AttemptSummary, RunSummary } from '../core/types.ts'
import { resolveFace, type StatusAttempts, type StatusEvents, type StatusSummary } from './face.ts'

/** Envelope every /dag-view JSON response carries. */
export type DagViewEnvelope<T> =
  | { ok: true; value: T }
  | { ok: false; error: { code: string; message: string } }

const OK = (value: unknown): { ok: true; value: unknown } => ({ ok: true, value })
const FAIL = (code: string, message: string): { ok: false; error: { code: string; message: string } } =>
  ({ ok: false, error: { code, message } })

/** Route prefix every endpoint hangs under. */
export const ROUTE_PREFIX = '/dag-view'

/** Request body size cap; larger bodies are destroyed rather than drained. */
const BODY_CAP_BYTES = 1 << 20

/** Read a JSON request body into an unknown value; null when unparseable/oversized. */
async function readJsonBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = []
  let total = 0
  for await (const chunk of req) {
    const part = chunk as Buffer
    total += part.length
    if (total > BODY_CAP_BYTES) {
      // Stop reading (no drain) and tear the connection down; the oversized
      // body is never parsed.
      req.destroy()
      chunks.length = 0
      return null
    }
    chunks.push(part)
  }
  const text = Buffer.concat(chunks).toString('utf8')
  if (text === '') return null
  try {
    return JSON.parse(text) as unknown
  } catch {
    return null
  }
}

/** Typed view of a decoded JSON object payload. */
function asRecord(payload: unknown): Record<string, unknown> | null {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return null
  return payload as Record<string, unknown>
}

/** Extract a non-empty string field, or null when absent/malformed. */
function stringField(payload: Record<string, unknown>, key: string): string | null {
  const value = payload[key]
  return typeof value === 'string' && value !== '' ? value : null
}

/** Extract an optional non-negative integer field, or undefined when absent/malformed. */
function optionalInt(payload: Record<string, unknown>, key: string): number | undefined {
  const value = payload[key]
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : undefined
}

/** Write one JSON envelope response. */
function json(res: ServerResponse, envelope: DagViewEnvelope<unknown>, status = 200): void {
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' })
  res.end(JSON.stringify(envelope))
}

/**
 * Map a thrown face error onto an envelope error. Messages starting with a
 * `dag.` code pass the code through (the orchestrator's stable codes:
 * `dag.run_not_found: <detail>`); anything else is an internal envelope so
 * raw host details never reach the browser.
 */
function faceErrorToEnvelope(error: unknown, logger: { warn: (message: string) => void }): { ok: false; error: { code: string; message: string } } {
  const message = error instanceof Error ? error.message : String(error)
  if (message.startsWith('dag.')) {
    // The face writes `"<code>: <detail>"` (and `.code`); the envelope code
    // is the message prefix before the separator, else the whole message.
    const separator = message.indexOf(' — ')
    const code = separator > 0 ? message.slice(0, separator) : message.split(':')[0].trim()
    return { ok: false, error: { code, message } }
  }
  logger.warn(`dsh-dag-view: face call failed: ${message}`)
  return FAIL('dag_view.internal', 'internal error')
}

/**
 * Register the /dag-view routes on the shared webserver.
 * @param ctx - context carrying webServer (and logger).
 * @returns the route disposer.
 */
export function registerDagViewRoutes(ctx: Context): () => void {
  const handler = async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    if (req.method !== 'POST') {
      res.writeHead(405)
      res.end()
      return
    }
    // CSRF hardening: every endpoint is read-only, but the JSON
    // content-type requirement keeps cross-site form posts out regardless.
    const contentType = req.headers['content-type'] ?? ''
    if (!contentType.toLowerCase().startsWith('application/json')) {
      res.writeHead(415)
      res.end()
      return
    }
    const pathname = new URL(req.url ?? '/', 'http://x').pathname
    const payload = await readJsonBody(req)
    const record = asRecord(payload)
    if (record === null) {
      json(res, FAIL('dag_view.bad_request', 'malformed JSON body'))
      return
    }

    // The face resolves per request: the orchestrator core plugin may be
    // absent; the UI degrades to the unavailable envelope, never throws.
    const resolved = resolveFace(ctx)
    if (!resolved.ok) {
      json(res, resolved)
      return
    }
    const { face } = resolved

    switch (pathname) {
      case '/dag-view/runs': {
        try {
          const projection = face.status(undefined, { detail: 'summary' }) as StatusSummary
          json(res, OK({ runs: projection.runs }))
        } catch (error) {
          json(res, faceErrorToEnvelope(error, ctx.logger))
        }
        return
      }
      case '/dag-view/run': {
        const runId = stringField(record, 'run_id')
        if (runId === null) {
          json(res, FAIL('dag_view.bad_request', 'missing run_id'))
          return
        }
        try {
          // One aggregate snapshot per round trip: spec + full attempt depth
          // + outputs. status(detail:'attempts') carries run/name/counts/
          // created_at/updated_at/tasks/attempts — split the base run fields
          // from tasks/attempts for the UI's run view.
          const [specRecord, projection, outputs] = await Promise.all([
            face.getSpec(runId),
            face.status(runId, { detail: 'attempts' }) as Promise<StatusAttempts>,
            face.listOutputs(runId),
          ])
          // Split the projection: run = the base run fields (kind/detail/
          // tasks/attempts peeled off), tasks/attempts for the graph views.
          const { kind: _kind, detail: _detail, tasks, attempts, ...run } = projection
          json(res, OK({ run, spec: specRecord.spec, tasks, attempts, outputs }))
        } catch (error) {
          json(res, faceErrorToEnvelope(error, ctx.logger))
          return
        }
        return
      }
      case '/dag-view/events': {
        const runId = stringField(record, 'run_id')
        if (runId === null) {
          json(res, FAIL('dag_view.bad_request', 'missing run_id'))
          return
        }
        const options: { detail: 'events'; afterSeq?: number; taskId?: string; limit?: number } = { detail: 'events' }
        const afterSeq = optionalInt(record, 'after_seq')
        if (afterSeq !== undefined) options.afterSeq = afterSeq
        const taskId = stringField(record, 'task_id')
        if (taskId !== null) options.taskId = taskId
        const limit = optionalInt(record, 'limit')
        if (limit !== undefined) options.limit = limit
        try {
          const projection = face.status(runId, options) as StatusEvents
          json(res, OK({ events: projection.events }))
        } catch (error) {
          json(res, faceErrorToEnvelope(error, ctx.logger))
        }
        return
      }
      case '/dag-view/attempt-logs': {
        const runId = stringField(record, 'run_id')
        if (runId === null) {
          json(res, FAIL('dag_view.bad_request', 'missing run_id'))
          return
        }
        try {
          const items: AttemptSummary[] = face.attemptSummaries(runId)
          json(res, OK({ items }))
        } catch (error) {
          json(res, faceErrorToEnvelope(error, ctx.logger))
        }
        return
      }
      case '/dag-view/session-runs': {
        const sessionId = stringField(record, 'session_id')
        if (sessionId === null) {
          json(res, FAIL('dag_view.bad_request', 'missing session_id'))
          return
        }
        try {
          // The GUI tab's session link: rows planned by this conversation.
          // An unknown session is a normal empty result, never an error.
          const projection = face.runsForSession(sessionId)
          json(res, OK({ runs: projection.runs }))
        } catch (error) {
          json(res, faceErrorToEnvelope(error, ctx.logger))
        }
        return
      }
      default:
        res.writeHead(404)
        res.end()
    }
  }

  return ctx.webServer.register({ kind: 'prefix', path: ROUTE_PREFIX, handler })
}
