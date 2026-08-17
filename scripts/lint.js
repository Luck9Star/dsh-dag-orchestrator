// lint — T24's core, brought forward (M6 of the M1 review fix batch).
//
// Four mechanical discipline checks, zero dependencies (node:test is NOT
// used here — the lint must run in bare CI runners and `npm run lint`):
//
//   1. `node --check` every lib/, scripts/, and test/*.js module (syntax).
//   2. sqlite outlet discipline (red lines R2/R7): `node:sqlite` /
//      `DatabaseSync` may appear in lib/ ONLY in dag-store.js (tests are
//      exempt — attacker-simulation tests deliberately open raw handles).
//   3. `db.exec(` allowlist: the only sanctioned call sites live in
//      lib/dag-store.js and each argument must be one of the module's
//      PRAGMA/DDL constants (PRAGMA ..., BEGIN IMMEDIATE, COMMIT, ROLLBACK,
//      DDL_SQL, or the PRAGMA_SETUP_SQL loop variable). Anything else in
//      lib/ — especially a string literal with interpolated user data —
//      is rejected.
//   4. @deepseek-ai/dsh-subagent import allowlist (red line 12, subagents
//      红线 12 同款): the only importing module in lib/ is executor.js and
//      the only imported member is assertSubagentMaxDepth.
//
// Comments are stripped before checks 2-4 (docs may NAME the forbidden
// tokens; only code is policed). Exit 0 = clean; exit 1 = violations.
//
// The pure helpers (stripComments / lintText) are exported for
// test/lint.test.js's positive+negative cases; the CLI entry runs only
// when this file is executed directly (`npm run lint`).

import { execFileSync } from 'node:child_process'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')

/**
 * Strip comments from source text, PRESERVING line numbers (each output
 * line corresponds 1:1 with the input line; comment content becomes
 * whitespace-empty). Handles /* ... *​/ block comments across lines, and
 * `//` line comments with a quote-parity guard: a `//` that appears inside
 * a string literal (e.g. the `task://...` refs this plugin's specs carry)
 * is NOT treated as a comment — the line is kept verbatim (conservative:
 * a real trailing comment after such a string survives too, which only
 * weakens the lint, never false-positives). Naive by design — a lint
 * heuristic, not a parser.
 *
 * @param {string} text
 * @returns {string[]}
 */
export function stripComments(text) {
  const lines = text.split('\n')
  let inBlock = false
  return lines.map((line) => {
    if (inBlock) {
      const end = line.indexOf('*/')
      if (end === -1) return ''
      inBlock = false
      return line.slice(end + 2)
    }
    const blockStart = line.indexOf('/*')
    if (blockStart !== -1) {
      const after = line.slice(blockStart + 2)
      const sameLineEnd = after.indexOf('*/')
      if (sameLineEnd !== -1) {
        return line.slice(0, blockStart) + after.slice(sameLineEnd + 2)
      }
      inBlock = true
      return line.slice(0, blockStart)
    }
    const lineComment = line.indexOf('//')
    if (lineComment !== -1) {
      const before = line.slice(0, lineComment)
      const singles = (before.match(/'/g) ?? []).length
      const doubles = (before.match(/"/g) ?? []).length
      if (singles % 2 === 0 && doubles % 2 === 0) return before
    }
    return line
  })
}

/**
 * db.exec() arguments sanctioned in lib/dag-store.js (module-constant
 * PRAGMA/DDL only — red line R7).
 */
export const SANCTIONED_EXEC_ARGS = new Set([
  "'PRAGMA busy_timeout = 5000'",
  '`PRAGMA application_id = ${DAG_APPLICATION_ID}`',
  '`PRAGMA user_version = ${DAG_USER_VERSION}`',
  'DDL_SQL',
  "'BEGIN IMMEDIATE'",
  "'COMMIT'",
  "'ROLLBACK'",
])

/** The one member lib/ may import from @deepseek-ai/dsh-subagent. */
export const SUBAGENT_MEMBER_ALLOWLIST = new Set(['assertSubagentMaxDepth'])

/**
 * Lint one file's TEXT (checks 2-4 — the static discipline rules).
 *
 * @param {string} file absolute path (used for violation labels)
 * @param {string} text the file's source
 * @returns {string[]} violations (`<check>: <file>: line N: <detail>`)
 */
export function lintText(file, text) {
  const violations = []
  const fail = (check, line, detail) => violations.push(`${check}: ${file}: line ${line}: ${detail}`)
  const code = stripComments(text)
  const isDagStore = file.endsWith(join('lib', 'dag-store.js'))
  const isExecutor = file.endsWith(join('lib', 'executor.js'))

  // 2. sqlite outlet discipline
  if (!isDagStore) {
    code.forEach((line, i) => {
      for (const token of ['node:sqlite', 'DatabaseSync']) {
        for (const match of line.matchAll(new RegExp(token, 'g'))) {
          fail('sqlite-outlet', i + 1, `"${token}" may only appear in lib/dag-store.js`)
        }
      }
    })
  }

  // 3. db.exec() argument discipline (lib/ only — the store is the sole outlet)
  code.forEach((line, i) => {
    for (const match of line.matchAll(/db\.exec\((.*?)\)/g)) {
      const arg = match[1].trim()
      if (/^pragma$/.test(arg)) continue // the PRAGMA_SETUP_SQL loop form
      if (!SANCTIONED_EXEC_ARGS.has(arg)) {
        fail('db.exec-allowlist', i + 1, `db.exec(${arg}) — only module-constant PRAGMA/DDL arguments are sanctioned`)
      }
    }
  })

  // 4. @deepseek-ai/dsh-subagent import allowlist
  code.forEach((line, i) => {
    for (const match of line.matchAll(/@deepseek-ai\/dsh-subagent/g)) {
      if (!/^\s*(import|export)\b/.test(line)) continue // prose mentions are fine
      if (isExecutor) {
        const members = [...line.matchAll(/\{([^}]*)\}/g)].flatMap((m) =>
          m[1].split(',').map((s) => s.trim().split(/\s+as\s+/)[0].trim()).filter(Boolean),
        )
        for (const member of members) {
          if (!SUBAGENT_MEMBER_ALLOWLIST.has(member)) {
            fail('dsh-subagent-allowlist', i + 1, `member "${member}" is outside the {assertSubagentMaxDepth} allowlist`)
          }
        }
        continue
      }
      fail('dsh-subagent-allowlist', i + 1, 'only lib/executor.js may import @deepseek-ai/dsh-subagent')
    }
  })

  return violations
}

/** Recursively list .js files under a directory (sorted for determinism). */
function jsFiles(dir) {
  const out = []
  let entries
  try {
    entries = readdirSync(dir)
  } catch {
    return out
  }
  for (const name of entries.sort()) {
    const full = join(dir, name)
    let st
    try {
      st = statSync(full)
    } catch {
      continue
    }
    if (st.isDirectory()) out.push(...jsFiles(full))
    else if (name.endsWith('.js')) out.push(full)
  }
  return out
}

/**
 * Run every check over the repo. Returns all violations (empty = clean).
 *
 * @returns {string[]}
 */
export function lintRepo() {
  const violations = []
  const libFiles = jsFiles(join(root, 'lib'))
  const testFiles = jsFiles(join(root, 'test'))
  const scriptFiles = jsFiles(join(root, 'scripts'))
  const all = [...libFiles, ...testFiles, ...scriptFiles]

  // 1. node --check every module (syntax)
  for (const file of all) {
    try {
      execFileSync(process.execPath, ['--check', file], { stdio: 'pipe' })
    } catch (error) {
      violations.push(`node --check: ${file}: ${String(error.stderr || error.message).trim().split('\n')[0]}`)
    }
  }

  // 2-4. static discipline on lib/ ONLY (R2/R7 govern lib/: tests simulate
  // attackers with raw DatabaseSync handles, scripts/ are tooling — the
  // discipline's subject is the plugin runtime surface).
  for (const file of libFiles) {
    violations.push(...lintText(file, readFileSync(file, 'utf8')))
  }
  return violations
}

// ---- CLI entry (direct execution only) ---------------------------------------
const isDirect = process.argv[1] !== undefined && (
  fileURLToPath(import.meta.url) === (
    (() => { try { return realpathSync(process.argv[1]) } catch { return process.argv[1] } })()
  )
)
if (isDirect) {
  const violations = lintRepo()
  if (violations.length > 0) {
    console.error(`lint: ${violations.length} violation(s)`)
    for (const v of violations) console.error(`  ${v}`)
    process.exit(1)
  }
  console.log('lint: clean')
}
