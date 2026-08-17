// DagStore — the single persistence outlet of dsh-dag-orchestrator (DESIGN §6).
//
// Discipline (red lines R2/R7; precedent: dsh-session-query-sqlite/lib/index.js):
//   * `node:sqlite` is loaded via dynamic import in an async factory; failure is
//     LOUD (no silent JSON fallback — red line 12).
//   * Database file: `wx`-created 0600 (+ chmod to defeat umask), WAL journal,
//     busy_timeout 5000, `application_id` ownership magic, `user_version = 1`.
//   * Every statement is a cached prepared statement with bound parameters;
//     user data NEVER lands in SQL string interpolation. `db.exec()` is only
//     ever called with module-constant PRAGMA/DDL strings (T24 lint will
//     enforce this mechanically — keep the discipline now).
//   * `tx(fn)` = BEGIN IMMEDIATE … COMMIT with ROLLBACK + rethrow on error;
//     nested transactions and async callbacks are rejected loud. Projection
//     writes and event inserts happen in the SAME transaction (invariants
//     #3/#6, DESIGN §2.1/§5.3).
//   * CAS-shaped methods return `{ok:true, ...}` / `{ok:false, reason}` and
//     never throw for a lost race; they DO throw loud for programming errors
//     (bad arguments, closed store, out-of-tx event insert).
//
// Schema: DESIGN §6.2 verbatim, six tables (runs/tasks/attempts/events/
// approvals/outputs) plus ONE sanctioned deviation: attempts.owner_token —
// see DDL comment below. No CHECK constraints: §6.2's SQL block carries none;
// transition legality belongs to the engine (T07) which drives the CAS
// from/to pairs, while this store only guarantees the mechanical CAS.
//
// Naming conventions on the API surface:
//   * Row-shaped arguments (insertRun/insertTasks/insertAttempt/insertApproval)
//     use snake_case keys mirroring the DDL columns exactly.
//   * insertEvent uses the task-brief shape {type, taskId?, attemptId?, payload}
//     (camelCase) with an optional `at` timestamp override for deterministic
//     replay/tests.

import { createHash, randomUUID } from 'node:crypto'
import { chmodSync, closeSync, mkdirSync, openSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

/** Ownership magic for dag-orchestrator database files: 0x44147D20 (1142193440). */
const DAG_APPLICATION_ID = 0x44147D20
/** Schema revision of the six-table layout (DESIGN §6.2). */
const DAG_USER_VERSION = 1
/** Genesis previous-hash: sha256 chain seed (DESIGN §6.3). */
const GENESIS_PREV_HASH = '0'.repeat(64)
/** Attempt states that may still transition to a terminal state. */
const ATTEMPT_NON_TERMINAL_STATES = ['claimed', 'running']
/** Attempt terminal states (a terminal attempt never regresses; retry = new ordinal). */
const ATTEMPT_TERMINAL_STATES = ['succeeded', 'failed', 'cancelled', 'orphaned']
/** Run terminal states (paused is resumable, hence non-terminal). */
const RUN_TERMINAL_STATES = ['succeeded', 'failed', 'cancelled']
/** Approval states (DESIGN §6.2). */
const APPROVAL_STATES = ['pending', 'approved', 'rejected']
/** Columns casRunState may patch (whitelist — never interpolate caller keys). */
const RUN_PATCH_COLUMNS = ['control_intent', 'parent_session', 'base_cwd', 'name']
/** Columns casTaskState may patch. */
const TASK_PATCH_COLUMNS = ['blocked_reason', 'retry_not_before']
/** Columns commitTerminal may patch alongside the terminal transition. */
const ATTEMPT_TERMINAL_PATCH_COLUMNS = ['stop_reason', 'failure_json', 'result_json']

// db.exec() is allowed ONLY for these module-constant strings (R7).
const PRAGMA_SETUP_SQL = [
  'PRAGMA journal_mode = WAL',
  'PRAGMA busy_timeout = 5000',
  `PRAGMA application_id = ${DAG_APPLICATION_ID}`,
  `PRAGMA user_version = ${DAG_USER_VERSION}`,
]

// DDL — DESIGN §6.2 verbatim (six tables), with the single sanctioned addition
// of attempts.owner_token (task brief T03 decision): it replaces task-weaver's
// persistent lease owner with the engine's in-memory in-flight handle id.
const DDL_SQL = `
CREATE TABLE IF NOT EXISTS runs (
  run_id         TEXT PRIMARY KEY,
  name           TEXT,
  spec_json      TEXT NOT NULL,
  spec_hash      TEXT NOT NULL,
  state          TEXT NOT NULL,
  control_intent TEXT,
  parent_session TEXT,
  base_cwd       TEXT NOT NULL,
  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL,
  version        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  run_id           TEXT NOT NULL,
  task_id          TEXT NOT NULL,
  state            TEXT NOT NULL,
  version          INTEGER NOT NULL,
  blocked_reason   TEXT,
  retry_not_before INTEGER,
  updated_at       INTEGER NOT NULL,
  PRIMARY KEY (run_id, task_id)
);

CREATE TABLE IF NOT EXISTS attempts (
  attempt_id   TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL,
  task_id      TEXT NOT NULL,
  ordinal      INTEGER NOT NULL,
  state        TEXT NOT NULL,          -- claimed|running|succeeded|failed|cancelled|orphaned
  backend      TEXT NOT NULL,
  child_session TEXT,                  -- SubagentRun.id (recovery-time session locator)
  owner_token  TEXT,                   -- replaces task-weaver lease owner: engine in-memory in-flight handle id (NULL until dispatch)
  started_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  stop_reason  TEXT,
  failure_json TEXT,
  result_json  TEXT,
  UNIQUE (run_id, task_id, ordinal)
);

CREATE TABLE IF NOT EXISTS events (
  event_id     TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL,
  seq          INTEGER NOT NULL,
  type         TEXT NOT NULL,
  at           INTEGER NOT NULL,
  task_id      TEXT,
  attempt_id   TEXT,
  payload_json TEXT NOT NULL,
  prev_hash    TEXT NOT NULL,
  hash         TEXT NOT NULL,
  UNIQUE (run_id, seq)                 -- seq monotonic per run = chain order
);

CREATE TABLE IF NOT EXISTS approvals (
  approval_id TEXT PRIMARY KEY,
  run_id      TEXT NOT NULL,
  task_id     TEXT NOT NULL,
  action      TEXT NOT NULL,
  state       TEXT NOT NULL,           -- pending|approved|rejected
  note        TEXT,
  created_at  INTEGER NOT NULL,
  decided_at  INTEGER
);

CREATE TABLE IF NOT EXISTS outputs (
  run_id              TEXT NOT NULL,
  task_id             TEXT NOT NULL,
  name                TEXT NOT NULL,
  value_json          TEXT NOT NULL,
  produced_by_attempt TEXT NOT NULL,
  created_at          INTEGER NOT NULL,
  PRIMARY KEY (run_id, task_id, name)
);
`

/**
 * Canonical JSON for stable hashing: object keys sorted lexicographically,
 * no whitespace, array order preserved, `undefined`-valued object keys dropped
 * (JSON.stringify semantics), top-level `undefined`/bigint/function/symbol
 * rejected loud. DESIGN §6.3 / task brief T03.
 */
export function canonicalJson(value) {
  if (value === undefined) throw new TypeError('canonicalJson: undefined is not representable')
  if (value === null) return 'null'
  const kind = typeof value
  if (kind === 'number' || kind === 'boolean' || kind === 'string') return JSON.stringify(value)
  if (kind === 'bigint') throw new TypeError('canonicalJson: bigint is not representable')
  if (kind === 'function' || kind === 'symbol') {
    throw new TypeError(`canonicalJson: ${kind} is not representable`)
  }
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(',')}]`
  const keys = Object.keys(value)
    .filter((key) => value[key] !== undefined)
    .sort()
  const body = keys.map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
  return `{${body.join(',')}}`
}

/** sha256(hex) of a UTF-8 string. */
function sha256Hex(input) {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

/** node:sqlite returns null-prototype rows — hand out plain objects instead. */
function plain(row) {
  return row === undefined || row === null ? null : { ...row }
}

function plainAll(rows) {
  return rows.map((row) => ({ ...row }))
}

/** undefined → null (node:sqlite rejects undefined binds); booleans never appear in this schema. */
function bindable(value) {
  return value === undefined ? null : value
}

function requireString(value, name) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string, got ${typeof value}`)
  }
  return value
}

function requireInteger(value, name) {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw new TypeError(`${name} must be a safe integer, got ${typeof value}`)
  }
  return value
}

/** Build a whitelisted `col = ?` patch list; unknown keys are loud (typo guard). */
function patchEntries(patch, allowedColumns, context) {
  if (patch === undefined || patch === null) return []
  if (typeof patch !== 'object' || Array.isArray(patch)) {
    throw new TypeError(`${context}: patch must be an object`)
  }
  const unknown = Object.keys(patch).filter((key) => !allowedColumns.includes(key))
  if (unknown.length > 0) {
    throw new TypeError(`${context}: unknown patch column(s) ${unknown.join(', ')}; allowed: ${allowedColumns.join(', ')}`)
  }
  return allowedColumns.filter((column) => Object.hasOwn(patch, column)).map((column) => [column, bindable(patch[column])])
}

function storeError(message, code, extra = {}) {
  const error = new Error(message)
  error.code = code
  Object.assign(error, extra)
  return error
}

/**
 * Guard an opened database: adopt empty files, accept owned databases at the
 * right user_version, reject everything else loud (`dag.store_ownership`).
 */
function guardOwnership(db, dbPath) {
  let applicationId
  let userVersion
  let tableCount
  try {
    applicationId = db.prepare('PRAGMA application_id').get().application_id
    userVersion = db.prepare('PRAGMA user_version').get().user_version
    tableCount = db
      .prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name NOT GLOB 'sqlite_*'")
      .get().n
  } catch (error) {
    // SQLite defers header validation to first pager access: a non-sqlite file
    // surfaces here as ERR_SQLITE_ERROR / SQLITE_NOTADB (code 26).
    if (error.code === 'ERR_SQLITE_ERROR' && error.errcode === 26) {
      throw storeError(
        `dag-store: file at "${dbPath}" is not a sqlite database (refusing to open: not a dag-orchestrator store)`,
        'dag.store_ownership',
      )
    }
    throw error
  }
  if (applicationId === DAG_APPLICATION_ID) {
    if (userVersion !== DAG_USER_VERSION) {
      throw storeError(
        `dag-store: database at "${dbPath}" has user_version ${userVersion}, expected ${DAG_USER_VERSION} (migration path not implemented)`,
        'dag.store_version',
      )
    }
    return
  }
  if (applicationId === 0 && tableCount === 0) return // fresh/empty file — adoptable
  if (applicationId === 0 && tableCount > 0) {
    throw storeError(
      `dag-store: database at "${dbPath}" is not empty and carries no dag-orchestrator application_id (refusing to adopt a foreign database)`,
      'dag.store_ownership',
    )
  }
  throw storeError(
    `dag-store: database at "${dbPath}" belongs to another application (application_id ${applicationId}, expected ${DAG_APPLICATION_ID})`,
    'dag.store_ownership',
  )
}

/**
 * Open (or create) the dag-orchestrator store.
 *
 * @param {{path: string}} options — file path (parent directories are created),
 *        or ':memory:' for an in-process database (journal stays 'memory').
 * @returns {Promise<object>} the store handle (async factory: node:sqlite is
 *          dynamically imported; failure rejects loud — no silent fallback).
 */
export async function createDagStore({ path } = {}) {
  if (typeof path !== 'string' || path.length === 0) {
    throw new TypeError('createDagStore: options.path must be a non-empty string')
  }
  let DatabaseSync
  try {
    ({ DatabaseSync } = await import('node:sqlite'))
  } catch (error) {
    throw storeError(
      `dag-store: node:sqlite is unavailable on this runtime — refusing to start without persistence (no silent fallback): ${error?.message ?? error}`,
      'dag.sqlite_unavailable',
      { cause: error },
    )
  }

  const isMemory = path === ':memory:'
  const dbPath = isMemory ? path : resolve(path)
  if (!isMemory) {
    mkdirSync(dirname(dbPath), { recursive: true, mode: 0o700 })
    let created = false
    try {
      // wx: refuse to clobber an existing file; 0600 owner-only from byte zero.
      const fd = openSync(dbPath, 'wx', 0o600)
      closeSync(fd)
      created = true
    } catch (error) {
      if (error.code !== 'EEXIST') throw error
    }
    if (created) chmodSync(dbPath, 0o600) // umask-proof belt and braces
  }

  const db = new DatabaseSync(dbPath)
  try {
    guardOwnership(db, dbPath)
    if (isMemory) {
      db.exec('PRAGMA busy_timeout = 5000')
      db.exec(`PRAGMA application_id = ${DAG_APPLICATION_ID}`)
      db.exec(`PRAGMA user_version = ${DAG_USER_VERSION}`)
    } else {
      for (const pragma of PRAGMA_SETUP_SQL) db.exec(pragma)
    }
    db.exec(DDL_SQL)
  } catch (error) {
    db.close()
    throw error
  }

  // ---- shared per-store state ------------------------------------------------
  const statements = new Map()
  /** Cached prepared statement (positions bind user data; SQL text is constant). */
  const stmt = (sql) => {
    let cached = statements.get(sql)
    if (cached === undefined) {
      cached = db.prepare(sql)
      statements.set(sql, cached)
    }
    return cached
  }
  let activeTx = false
  let closed = false

  const assertOpen = () => {
    if (closed) throw storeError('dag-store: store is closed', 'dag.store_closed')
  }

  // ---- runs ------------------------------------------------------------------

  function insertRun(row) {
    assertOpen()
    const source = row ?? {}
    requireString(source.run_id, 'insertRun: run_id')
    requireString(source.spec_json, 'insertRun: spec_json')
    requireString(source.spec_hash, 'insertRun: spec_hash')
    requireString(source.state, 'insertRun: state')
    requireString(source.base_cwd, 'insertRun: base_cwd')
    const now = Date.now()
    stmt(
      'INSERT INTO runs (run_id, name, spec_json, spec_hash, state, control_intent, parent_session, base_cwd, created_at, updated_at, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    ).run(
      source.run_id,
      bindable(source.name),
      source.spec_json,
      source.spec_hash,
      source.state,
      bindable(source.control_intent),
      bindable(source.parent_session),
      source.base_cwd,
      source.created_at ?? now,
      source.updated_at ?? now,
      source.version ?? 1,
    )
    return findRun(source.run_id)
  }

  function findRun(runId) {
    assertOpen()
    requireString(runId, 'findRun: runId')
    return plain(stmt('SELECT * FROM runs WHERE run_id = ?').get(runId))
  }

  function findNonTerminalRuns() {
    assertOpen()
    return plainAll(
      stmt(
        `SELECT * FROM runs WHERE state NOT IN (${RUN_TERMINAL_STATES.map(() => '?').join(', ')}) ORDER BY created_at, run_id`,
      ).all(...RUN_TERMINAL_STATES),
    )
  }

  /**
   * ALL runs, terminal included, oldest first (T08: dag_status's omit-run_id
   * arm lists every run — terminal runs are the common case there, so the
   * non-terminal helper is the wrong source). Backward-compatible addition;
   * engine.allRunIds consumes it to make the all-runs summary complete
   * without the in-memory knownRunIds registry.
   */
  function findAllRuns() {
    assertOpen()
    return plainAll(stmt('SELECT * FROM runs ORDER BY created_at, run_id').all())
  }

  function casRunState(runId, fromState, version, to, patch) {
    assertOpen()
    requireString(runId, 'casRunState: runId')
    requireString(fromState, 'casRunState: fromState')
    requireInteger(version, 'casRunState: version')
    requireString(to, 'casRunState: to')
    const entries = patchEntries(patch, RUN_PATCH_COLUMNS, 'casRunState')
    const setSql = ['state = ?', 'version = version + 1', 'updated_at = ?', ...entries.map(([column]) => `${column} = ?`)].join(', ')
    const result = stmt(`UPDATE runs SET ${setSql} WHERE run_id = ? AND state = ? AND version = ?`).run(
      to,
      Date.now(),
      ...entries.map(([, value]) => value),
      runId,
      fromState,
      version,
    )
    if (result.changes === 1) return { ok: true, row: findRun(runId) }
    const current = findRun(runId)
    if (current === null) return { ok: false, reason: 'not_found' }
    return { ok: false, reason: 'cas_mismatch', current: { state: current.state, version: current.version } }
  }

  function setControlIntent(runId, intent) {
    assertOpen()
    requireString(runId, 'setControlIntent: runId')
    if (intent !== undefined && intent !== null) requireString(intent, 'setControlIntent: intent')
    const result = stmt('UPDATE runs SET control_intent = ?, updated_at = ? WHERE run_id = ?').run(
      bindable(intent),
      Date.now(),
      runId,
    )
    if (result.changes === 0) return { ok: false, reason: 'not_found' }
    return { ok: true, row: findRun(runId) }
  }

  // ---- tasks -----------------------------------------------------------------

  function insertTasks(runId, taskRows) {
    assertOpen()
    requireString(runId, 'insertTasks: runId')
    if (!Array.isArray(taskRows)) throw new TypeError('insertTasks: taskRows must be an array')
    const insert = stmt(
      'INSERT INTO tasks (run_id, task_id, state, version, blocked_reason, retry_not_before, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
    )
    const now = Date.now()
    for (const source of taskRows) {
      requireString(source?.task_id, 'insertTasks: taskRows[].task_id')
      insert.run(
        runId,
        source.task_id,
        source.state ?? 'pending',
        source.version ?? 1,
        bindable(source.blocked_reason),
        bindable(source.retry_not_before),
        source.updated_at ?? now,
      )
    }
    return findTasks(runId)
  }

  function findTasks(runId) {
    assertOpen()
    requireString(runId, 'findTasks: runId')
    return plainAll(stmt('SELECT * FROM tasks WHERE run_id = ? ORDER BY task_id').all(runId))
  }

  /** tasks in `ready` state (dispatch candidates — the buildQueue input). */
  function findReadyTasks(runId) {
    assertOpen()
    requireString(runId, 'findReadyTasks: runId')
    return plainAll(stmt('SELECT * FROM tasks WHERE run_id = ? AND state = ? ORDER BY task_id').all(runId, 'ready'))
  }

  function casTaskState(runId, taskId, fromState, version, to, patch) {
    assertOpen()
    requireString(runId, 'casTaskState: runId')
    requireString(taskId, 'casTaskState: taskId')
    requireString(fromState, 'casTaskState: fromState')
    requireInteger(version, 'casTaskState: version')
    requireString(to, 'casTaskState: to')
    const entries = patchEntries(patch, TASK_PATCH_COLUMNS, 'casTaskState')
    const setSql = ['state = ?', 'version = version + 1', 'updated_at = ?', ...entries.map(([column]) => `${column} = ?`)].join(', ')
    const result = stmt(
      `UPDATE tasks SET ${setSql} WHERE run_id = ? AND task_id = ? AND state = ? AND version = ?`,
    ).run(to, Date.now(), ...entries.map(([, value]) => value), runId, taskId, fromState, version)
    if (result.changes === 1) {
      return { ok: true, row: plain(stmt('SELECT * FROM tasks WHERE run_id = ? AND task_id = ?').get(runId, taskId)) }
    }
    const current = stmt('SELECT * FROM tasks WHERE run_id = ? AND task_id = ?').get(runId, taskId)
    if (current === undefined) return { ok: false, reason: 'not_found' }
    return { ok: false, reason: 'cas_mismatch', current: { state: current.state, version: current.version } }
  }

  // ---- attempts ----------------------------------------------------------------

  /**
   * Record the dispatched child session on an attempt (T07 engine: after a
   * successful executor.dispatch, OUTSIDE the claim transaction). Projection
   * change + `attempt.dispatched` event land in ONE caller-supplied
   * transaction (invariant #6). The attempt must still be non-terminal.
   *
   * @param {string} attemptId
   * @param {string} childSession SubagentRun.id
   */
  function updateAttemptChildSession(attemptId, childSession) {
    assertOpen()
    requireString(attemptId, 'updateAttemptChildSession: attemptId')
    requireString(childSession, 'updateAttemptChildSession: childSession')
    const result = stmt(
      `UPDATE attempts SET child_session = ?, updated_at = ? WHERE attempt_id = ? AND state IN (${ATTEMPT_NON_TERMINAL_STATES.map(() => '?').join(', ')})`,
    ).run(childSession, Date.now(), attemptId, ...ATTEMPT_NON_TERMINAL_STATES)
    if (result.changes === 1) return { ok: true, row: findAttempt(attemptId) }
    const current = findAttempt(attemptId)
    if (current === null) return { ok: false, reason: 'not_found' }
    return { ok: false, reason: 'not_in_flight', attempt: current }
  }

  function insertAttempt(row) {
    assertOpen()
    const source = row ?? {}
    requireString(source.attempt_id, 'insertAttempt: attempt_id')
    requireString(source.run_id, 'insertAttempt: run_id')
    requireString(source.task_id, 'insertAttempt: task_id')
    requireInteger(source.ordinal ?? 1, 'insertAttempt: ordinal')
    requireString(source.backend, 'insertAttempt: backend')
    const now = Date.now()
    stmt(
      'INSERT INTO attempts (attempt_id, run_id, task_id, ordinal, state, backend, child_session, owner_token, started_at, updated_at, stop_reason, failure_json, result_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    ).run(
      source.attempt_id,
      source.run_id,
      source.task_id,
      source.ordinal ?? 1,
      source.state ?? 'claimed',
      source.backend,
      bindable(source.child_session),
      bindable(source.owner_token),
      source.started_at ?? now,
      source.updated_at ?? now,
      bindable(source.stop_reason),
      bindable(source.failure_json),
      bindable(source.result_json),
    )
    return findAttempt(source.attempt_id)
  }

  function findAttempt(attemptId) {
    assertOpen()
    requireString(attemptId, 'findAttempt: attemptId')
    return plain(stmt('SELECT * FROM attempts WHERE attempt_id = ?').get(attemptId))
  }

  function findAttempts(runId, taskId) {
    assertOpen()
    requireString(runId, 'findAttempts: runId')
    requireString(taskId, 'findAttempts: taskId')
    return plainAll(stmt('SELECT * FROM attempts WHERE run_id = ? AND task_id = ? ORDER BY ordinal').all(runId, taskId))
  }

  function findNonTerminalAttempts(runId) {
    assertOpen()
    requireString(runId, 'findNonTerminalAttempts: runId')
    return plainAll(
      stmt(
        `SELECT * FROM attempts WHERE run_id = ? AND state IN (${ATTEMPT_NON_TERMINAL_STATES.map(() => '?').join(', ')}) ORDER BY started_at, attempt_id`,
      ).all(runId, ...ATTEMPT_NON_TERMINAL_STATES),
    )
  }

  function hasNonTerminalAttempt(runId, taskId) {
    assertOpen()
    requireString(runId, 'hasNonTerminalAttempt: runId')
    requireString(taskId, 'hasNonTerminalAttempt: taskId')
    const row = stmt(
      `SELECT 1 AS hit FROM attempts WHERE run_id = ? AND task_id = ? AND state IN (${ATTEMPT_NON_TERMINAL_STATES.map(() => '?').join(', ')}) LIMIT 1`,
    ).get(runId, taskId, ...ATTEMPT_NON_TERMINAL_STATES)
    return row !== undefined
  }

  /**
   * Commit one attempt to a terminal state — the T03 analogue of task-weaver's
   * `commitTerminal` (terminal-commit.ts): guarded by BOTH the state CAS
   * (`state IN ('claimed','running')` — an attempt already terminal never
   * regresses) AND the owner token (the engine's in-memory in-flight handle id;
   * a stale handle must not commit over a fresher claim). Losing either race
   * returns not-ok; it never throws.
   *
   * @param {string} attemptId
   * @param {string|null} ownerId — owner_token the caller holds (null matches a NULL token).
   * @param {string} target — succeeded|failed|cancelled|orphaned.
   * @param {{stop_reason?: string|null, failure_json?: string|null, result_json?: string|null}} [patch]
   */
  function commitTerminal(attemptId, ownerId, target, patch) {
    assertOpen()
    requireString(attemptId, 'commitTerminal: attemptId')
    if (ownerId !== undefined && ownerId !== null) requireString(ownerId, 'commitTerminal: ownerId')
    if (!ATTEMPT_TERMINAL_STATES.includes(target)) {
      throw new TypeError(`commitTerminal: target must be one of ${ATTEMPT_TERMINAL_STATES.join('|')}, got ${JSON.stringify(target)}`)
    }
    const entries = patchEntries(patch, ATTEMPT_TERMINAL_PATCH_COLUMNS, 'commitTerminal')
    const setSql = ['state = ?', 'updated_at = ?', ...entries.map(([column]) => `${column} = ?`)].join(', ')
    const result = stmt(
      `UPDATE attempts SET ${setSql} WHERE attempt_id = ? AND state IN (${ATTEMPT_NON_TERMINAL_STATES.map(() => '?').join(', ')}) AND owner_token IS ?`,
    ).run(target, Date.now(), ...entries.map(([, value]) => value), attemptId, ...ATTEMPT_NON_TERMINAL_STATES, bindable(ownerId))
    if (result.changes === 1) return { ok: true, row: findAttempt(attemptId) }
    const current = findAttempt(attemptId)
    if (current === null) return { ok: false, reason: 'not_found' }
    if (!ATTEMPT_NON_TERMINAL_STATES.includes(current.state)) {
      return { ok: false, reason: 'not_in_flight', attempt: current }
    }
    return { ok: false, reason: 'owner_mismatch', attempt: current }
  }

  // ---- events (hash chain) -----------------------------------------------------

  /**
   * Append one event to the run's chain. MUST be called inside `tx()` — the
   * seq allocation, hash computation and projection writes have to land in the
   * same transaction or the chain could fork (invariants #3/#6, mechanically
   * enforced here). Computes:
   *   seq = last.seq + 1 (1-based)
   *   prev_hash = last.hash, or '0'×64 for the genesis event
   *   hash = sha256(prev_hash ∥ '\n' ∥ canonical_json({seq, type, at, task_id?, attempt_id?, payload}))
   *
   * @param {string} runId
   * @param {{type: string, taskId?: string, attemptId?: string, payload: unknown, at?: number}} event
   * @returns the inserted event row (plain object)
   */
  function insertEvent(runId, event) {
    assertOpen()
    if (!activeTx) {
      throw storeError(
        'dag-store: insertEvent must be called inside store.tx() — seq/hash chain progression and projection writes are only atomic together',
        'dag.tx_required',
      )
    }
    requireString(runId, 'insertEvent: runId')
    requireString(event?.type, 'insertEvent: event.type')
    if (event.payload === undefined) {
      throw new TypeError('insertEvent: event.payload is required (use null or {} for an empty payload)')
    }
    const taskId = event.taskId === undefined ? null : requireString(event.taskId, 'insertEvent: event.taskId')
    const attemptId = event.attemptId === undefined ? null : requireString(event.attemptId, 'insertEvent: event.attemptId')
    const payloadJson = canonicalJson(event.payload)
    const at = event.at === undefined ? Date.now() : requireInteger(event.at, 'insertEvent: event.at')
    const last = stmt('SELECT seq, hash FROM events WHERE run_id = ? ORDER BY seq DESC LIMIT 1').get(runId)
    const seq = (last ? last.seq : 0) + 1
    const prevHash = last ? last.hash : GENESIS_PREV_HASH
    const hashed = { seq, type: event.type, at }
    if (taskId !== null) hashed.task_id = taskId
    if (attemptId !== null) hashed.attempt_id = attemptId
    hashed.payload = event.payload
    const hash = sha256Hex(`${prevHash}\n${canonicalJson(hashed)}`)
    const eventId = randomUUID()
    stmt(
      'INSERT INTO events (event_id, run_id, seq, type, at, task_id, attempt_id, payload_json, prev_hash, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    ).run(eventId, runId, seq, event.type, at, taskId, attemptId, payloadJson, prevHash, hash)
    return {
      event_id: eventId,
      run_id: runId,
      seq,
      type: event.type,
      at,
      task_id: taskId,
      attempt_id: attemptId,
      payload_json: payloadJson,
      prev_hash: prevHash,
      hash,
    }
  }

  function findEvents(runId, options = {}) {
    assertOpen()
    requireString(runId, 'findEvents: runId')
    const afterSeq = options.afterSeq === undefined ? 0 : requireInteger(options.afterSeq, 'findEvents: options.afterSeq')
    if (options.limit === undefined) {
      return plainAll(stmt('SELECT * FROM events WHERE run_id = ? AND seq > ? ORDER BY seq').all(runId, afterSeq))
    }
    return plainAll(
      stmt('SELECT * FROM events WHERE run_id = ? AND seq > ? ORDER BY seq LIMIT ?').all(
        runId,
        afterSeq,
        requireInteger(options.limit, 'findEvents: options.limit'),
      ),
    )
  }

  /**
   * Recompute the whole chain for a run and compare against stored rows.
   * Detects tampered payloads/hashes, deleted events (seq contiguity + prev_hash
   * linkage) and reordered seqs. Read-only; safe outside tx.
   *
   * @returns {{ok: true}} or {{ok: false, firstBadSeq: number}}
   */
  function verifyChain(runId) {
    assertOpen()
    requireString(runId, 'verifyChain: runId')
    const rows = stmt(
      'SELECT seq, type, at, task_id, attempt_id, payload_json, prev_hash, hash FROM events WHERE run_id = ? ORDER BY seq',
    ).all(runId)
    let expectedSeq = 1
    let expectedPrevHash = GENESIS_PREV_HASH
    for (const row of rows) {
      if (row.seq !== expectedSeq || row.prev_hash !== expectedPrevHash) {
        return { ok: false, firstBadSeq: row.seq }
      }
      let payload
      try {
        payload = JSON.parse(row.payload_json)
      } catch {
        return { ok: false, firstBadSeq: row.seq } // payload_json is not valid JSON at all
      }
      const hashed = { seq: row.seq, type: row.type, at: row.at }
      if (row.task_id !== null) hashed.task_id = row.task_id
      if (row.attempt_id !== null) hashed.attempt_id = row.attempt_id
      hashed.payload = payload
      if (sha256Hex(`${row.prev_hash}\n${canonicalJson(hashed)}`) !== row.hash) {
        return { ok: false, firstBadSeq: row.seq }
      }
      expectedSeq += 1
      expectedPrevHash = row.hash
    }
    return { ok: true }
  }

  // ---- approvals -----------------------------------------------------------------

  function insertApproval(row) {
    assertOpen()
    const source = row ?? {}
    requireString(source.approval_id, 'insertApproval: approval_id')
    requireString(source.run_id, 'insertApproval: run_id')
    requireString(source.task_id, 'insertApproval: task_id')
    requireString(source.action, 'insertApproval: action')
    const state = source.state ?? 'pending'
    if (!APPROVAL_STATES.includes(state)) {
      throw new TypeError(`insertApproval: state must be one of ${APPROVAL_STATES.join('|')}, got ${JSON.stringify(state)}`)
    }
    stmt(
      'INSERT INTO approvals (approval_id, run_id, task_id, action, state, note, created_at, decided_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
    ).run(
      source.approval_id,
      source.run_id,
      source.task_id,
      source.action,
      state,
      bindable(source.note),
      source.created_at ?? Date.now(),
      bindable(source.decided_at),
    )
    return plain(stmt('SELECT * FROM approvals WHERE approval_id = ?').get(source.approval_id))
  }

  function findPendingApprovals(runId) {
    assertOpen()
    requireString(runId, 'findPendingApprovals: runId')
    return plainAll(stmt('SELECT * FROM approvals WHERE run_id = ? AND state = ? ORDER BY created_at, approval_id').all(runId, 'pending'))
  }

  /**
   * EVERY approval row of one (run, task) pair, every state, oldest first
   * (T12: the executor's three-branch lookup — already-approved /
   * already-rejected / pending-reuse — needs the full decision history the
   * pending-only helper cannot answer; findPendingApprovals stays untouched
   * for the reconcile + waiting_on paths).
   *
   * @param {string} runId
   * @param {string} taskId
   */
  function findApprovalsByTask(runId, taskId) {
    assertOpen()
    requireString(runId, 'findApprovalsByTask: runId')
    requireString(taskId, 'findApprovalsByTask: taskId')
    return plainAll(
      stmt('SELECT * FROM approvals WHERE run_id = ? AND task_id = ? ORDER BY created_at, approval_id').all(runId, taskId),
    )
  }

  /**
   * The run's DECIDED approvals (state approved/rejected), oldest decision
   * first (T12 reconcile source): a decided row no longer appears in
   * findPendingApprovals, so reconcileApprovals needs exactly the rows that
   * LEFT the pending set. Idempotence belongs to the caller's task CAS —
   * re-reading a decided row is free.
   *
   * @param {string} runId
   */
  function findDecidedApprovals(runId) {
    assertOpen()
    requireString(runId, 'findDecidedApprovals: runId')
    return plainAll(
      stmt("SELECT * FROM approvals WHERE run_id = ? AND state IN ('approved', 'rejected') ORDER BY decided_at, approval_id").all(runId),
    )
  }

  /**
   * Decide a pending approval. CAS on state='pending': an already-decided (or
   * unknown) approval returns not-ok instead of throwing. A null/undefined
   * note keeps the existing note (COALESCE).
   *
   * @param {string} approvalId
   * @param {'approved'|'rejected'} decision — the stored state value (the
   *        dag_approve tool's approve/reject wording is mapped by the engine).
   * @param {string} [note]
   */
  function decideApproval(approvalId, decision, note) {
    assertOpen()
    requireString(approvalId, 'decideApproval: approvalId')
    if (!['approved', 'rejected'].includes(decision)) {
      throw new TypeError(`decideApproval: decision must be approved|rejected, got ${JSON.stringify(decision)}`)
    }
    const result = stmt(
      "UPDATE approvals SET state = ?, note = COALESCE(?, note), decided_at = ? WHERE approval_id = ? AND state = 'pending'",
    ).run(decision, bindable(note), Date.now(), approvalId)
    if (result.changes === 1) return { ok: true, row: plain(stmt('SELECT * FROM approvals WHERE approval_id = ?').get(approvalId)) }
    const current = plain(stmt('SELECT * FROM approvals WHERE approval_id = ?').get(approvalId))
    if (current === null) return { ok: false, reason: 'not_found' }
    return { ok: false, reason: 'already_decided', approval: current }
  }

  // ---- outputs ---------------------------------------------------------------------

  function upsertOutput(runId, taskId, name, valueJson, attemptId) {
    assertOpen()
    requireString(runId, 'upsertOutput: runId')
    requireString(taskId, 'upsertOutput: taskId')
    requireString(name, 'upsertOutput: name')
    if (typeof valueJson !== 'string') throw new TypeError('upsertOutput: valueJson must be a string (canonical JSON)')
    requireString(attemptId, 'upsertOutput: attemptId')
    stmt(
      `INSERT INTO outputs (run_id, task_id, name, value_json, produced_by_attempt, created_at)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT (run_id, task_id, name) DO UPDATE SET
         value_json = excluded.value_json,
         produced_by_attempt = excluded.produced_by_attempt,
         created_at = excluded.created_at`,
    ).run(runId, taskId, name, valueJson, attemptId, Date.now())
    return findOutput(runId, taskId, name)
  }

  function findOutput(runId, taskId, name) {
    assertOpen()
    requireString(runId, 'findOutput: runId')
    requireString(taskId, 'findOutput: taskId')
    requireString(name, 'findOutput: name')
    return plain(stmt('SELECT * FROM outputs WHERE run_id = ? AND task_id = ? AND name = ?').get(runId, taskId, name))
  }

  function findOutputsByTask(runId, taskId) {
    assertOpen()
    requireString(runId, 'findOutputsByTask: runId')
    requireString(taskId, 'findOutputsByTask: taskId')
    return plainAll(stmt('SELECT * FROM outputs WHERE run_id = ? AND task_id = ? ORDER BY name').all(runId, taskId))
  }

  /**
   * ALL output rows of EVERY run, oldest run first then task/name order —
   * the recovery-time orphan audit scan (DESIGN §12.1's `inconsistent`
   * residue: outputs rows whose producing attempt never reached a terminal
   * state). Read-only; safe outside tx.
   */
  function findAllOutputs() {
    assertOpen()
    return plainAll(stmt('SELECT * FROM outputs ORDER BY run_id, task_id, name').all())
  }

  // ---- transactions ------------------------------------------------------------------

  /**
   * Synchronous immediate transaction. BEGIN IMMEDIATE takes the write lock up
   * front (single-writer discipline, DESIGN §6.4). Nested calls and async
   * callbacks are rejected loud — DatabaseSync makes in-process transactions
   * naturally serialized.
   *
   * @template T
   * @param {() => T} fn
   * @returns {T} whatever fn returned
   */
  function tx(fn) {
    assertOpen()
    if (typeof fn !== 'function') throw new TypeError('tx: fn must be a function')
    if (activeTx) {
      throw storeError('dag-store: nested transaction rejected — transactions serialize in-process', 'dag.nested_tx')
    }
    activeTx = true
    let result
    try {
      db.exec('BEGIN IMMEDIATE')
      result = fn()
      if (result !== null && typeof result === 'object' && typeof result.then === 'function') {
        throw storeError(
          'dag-store: tx callback returned a promise — transactions are synchronous; awaiting inside tx would break atomicity',
          'dag.async_tx',
        )
      }
      db.exec('COMMIT')
    } catch (error) {
      try {
        db.exec('ROLLBACK')
      } catch (rollbackError) {
        // BEGIN itself can fail (e.g. busy timeout) — then there is nothing to
        // roll back; surface the original error with the rollback attached.
        error.rollbackError = rollbackError
      }
      throw error
    } finally {
      activeTx = false
    }
    return result
  }

  /** Close the database (idempotent). Cached statements are finalized with it. */
  function close() {
    if (closed) return
    closed = true
    db.close()
  }

  return {
    tx,
    close,
    // runs
    insertRun,
    findRun,
    findNonTerminalRuns,
    findAllRuns,
    casRunState,
    setControlIntent,
    // tasks
    insertTasks,
    findTasks,
    findReadyTasks,
    casTaskState,
    // attempts
    insertAttempt,
    findAttempt,
    findAttempts,
    findNonTerminalAttempts,
    hasNonTerminalAttempt,
    updateAttemptChildSession,
    commitTerminal,
    // events
    insertEvent,
    findEvents,
    verifyChain,
    // approvals
    insertApproval,
    findPendingApprovals,
    findApprovalsByTask,
    findDecidedApprovals,
    decideApproval,
    // outputs
    upsertOutput,
    findOutput,
    findOutputsByTask,
    findAllOutputs,
  }
}
