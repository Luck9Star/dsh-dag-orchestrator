/**
 * lint tests — TASKS.md T24 (brought forward as review Major M6):
 * positive + negative cases for scripts/lint.js's mechanical discipline.
 *
 * The lint itself must run green over THIS repo (the CLI probe spawns
 * `node scripts/lint.js` as a subprocess); the negative cases feed
 * deliberately-violating sources to the exported lintText/stripComments
 * helpers and assert each check fires with the right label.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

import { lintText, stripComments } from '../scripts/lint.js'

const here = dirname(fileURLToPath(import.meta.url))

/** Run the real CLI and return {code, output}. */
function runLintCli() {
  try {
    const output = execFileSync(process.execPath, [join(here, '..', 'scripts', 'lint.js')], {
      stdio: 'pipe',
      encoding: 'utf8',
    })
    return { code: 0, output }
  } catch (error) {
    return { code: error.status ?? 1, output: String(error.stdout ?? '') + String(error.stderr ?? '') }
  }
}

// ---------------------------------------------------------------------------
// the repo itself is clean (the CI contract)
// ---------------------------------------------------------------------------

test('lint: the repository itself passes (npm run lint contract)', () => {
  const { code, output } = runLintCli()
  assert.equal(code, 0, `lint reported violations:\n${output}`)
  assert.match(output, /lint: clean/)
})

// ---------------------------------------------------------------------------
// stripComments
// ---------------------------------------------------------------------------

test('lint helpers: stripComments blanks comments, keeps code and line numbers', () => {
  const lines = stripComments([
    '// full line comment',
    'const a = 1; // trailing comment',
    'const url = "task://producer/name"; // trailing after a //-bearing string',
    '/* block start',
    'still inside block */ const b = 2;',
    'const c = 3;',
  ].join('\n'))
  assert.equal(lines.length, 6)
  assert.equal(lines[0], '')
  assert.equal(lines[1], 'const a = 1; ')
  // Conservative quote-parity: a // INSIDE a string is never a comment —
  // the line is kept verbatim (a real trailing comment after such a string
  // survives too; that only weakens the lint, never false-positives).
  assert.equal(lines[2], 'const url = "task://producer/name"; // trailing after a //-bearing string')
  assert.equal(lines[3], '')
  assert.equal(lines[4].includes('const b = 2;'), true)
  assert.equal(lines[4].includes('block'), false)
  assert.equal(lines[5], 'const c = 3;')
})

// ---------------------------------------------------------------------------
// check 2 — sqlite outlet discipline
// ---------------------------------------------------------------------------

test('lint: node:sqlite outside dag-store.js is a violation (R2/R7)', () => {
  const violations = lintText(
    '/repo/lib/sneaky.js',
    'import { DatabaseSync } from "node:sqlite"\nexport const x = 1\n',
  )
  assert.equal(violations.length, 2) // node:sqlite AND DatabaseSync
  assert.match(violations[0], /sqlite-outlet/)
  assert.match(violations[0], /node:sqlite/)
  assert.match(violations[1], /sqlite-outlet/)
  assert.match(violations[1], /DatabaseSync/)
})

test('lint: node:sqlite INSIDE dag-store.js is sanctioned (positive case)', () => {
  const violations = lintText(
    '/repo/lib/dag-store.js',
    'const { DatabaseSync } = await import("node:sqlite")\n',
  )
  assert.deepEqual(violations, [])
})

test('lint: doc comments NAMING sqlite outside dag-store do not trip the check', () => {
  const violations = lintText(
    '/repo/lib/doc.js',
    '// This module never touches node:sqlite or DatabaseSync — the store does.\nexport const x = 1\n',
  )
  assert.deepEqual(violations, [], 'comments are prose, not code')
})

// ---------------------------------------------------------------------------
// check 3 — db.exec() argument allowlist
// ---------------------------------------------------------------------------

test('lint: db.exec with a non-constant argument is a violation (R7)', () => {
  const violations = lintText(
    '/repo/lib/evil.js',
    'db.exec(`SELECT * FROM runs WHERE run_id = ${runId}`)\n',
  )
  assert.equal(violations.length, 1)
  assert.match(violations[0], /db\.exec-allowlist/)
  assert.match(violations[0], /module-constant/)
})

test('lint: the sanctioned db.exec forms pass (positive case)', () => {
  const violations = lintText(
    '/repo/lib/dag-store.js',
    [
      'for (const pragma of PRAGMA_SETUP_SQL) db.exec(pragma)',
      "db.exec('BEGIN IMMEDIATE')",
      "db.exec('COMMIT')",
      "db.exec('ROLLBACK')",
      'db.exec(DDL_SQL)',
      "`PRAGMA application_id = ${DAG_APPLICATION_ID}`",
    ].join('\n'),
  )
  assert.deepEqual(violations, [])
})

// ---------------------------------------------------------------------------
// check 4 — @deepseek-ai/dsh-subagent import allowlist
// ---------------------------------------------------------------------------

test('lint: dsh-subagent import outside executor.js is a violation (red line 12)', () => {
  const violations = lintText(
    '/repo/lib/index.js',
    "import { startSubagent } from '@deepseek-ai/dsh-subagent'\n",
  )
  assert.equal(violations.length, 1)
  assert.match(violations[0], /dsh-subagent-allowlist/)
  assert.match(violations[0], /only lib\/executor\.js/)
})

test('lint: a non-allowlisted member inside executor.js is a violation', () => {
  const violations = lintText(
    '/repo/lib/executor.js',
    "import { assertSubagentMaxDepth, listSubagents } from '@deepseek-ai/dsh-subagent'\n",
  )
  assert.equal(violations.length, 1)
  assert.match(violations[0], /dsh-subagent-allowlist/)
  assert.match(violations[0], /assertSubagentMaxDepth/)
})

test('lint: the whitelisted import inside executor.js passes (positive case)', () => {
  const violations = lintText(
    '/repo/lib/executor.js',
    "import { assertSubagentMaxDepth } from '@deepseek-ai/dsh-subagent';\n",
  )
  assert.deepEqual(violations, [])
})

test('lint: prose mentions of dsh-subagent outside imports do not trip the check', () => {
  const violations = lintText(
    '/repo/lib/engine.js',
    [
      '// The @deepseek-ai/dsh-subagent import whitelist lives in executor.js.',
      'export const x = 1',
    ].join('\n'),
  )
  assert.deepEqual(violations, [])
})
