import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'

// T01 scaffold smoke test (dot-prefixed so later tasks' real suites sort
// ahead of it): proves the `node --test` wiring runs on a nearly-empty repo.
// The real test suite lands with T02+.

test('T01 scaffold: package.json is a valid dsh-dag-orchestrator manifest', async () => {
  const raw = await readFile(new URL('../package.json', import.meta.url), 'utf8')
  const pkg = JSON.parse(raw)
  assert.equal(pkg.name, 'dsh-dag-orchestrator')
  assert.equal(pkg.type, 'module')
  assert.equal(pkg.dsh.bundle.patch, './cordis.patch.yml')
})

test('T01 scaffold: node runtime satisfies engines (>=22.13, node:sqlite)', () => {
  const [major, minor] = process.versions.node.split('.').map(Number)
  assert.ok(major > 22 || (major === 22 && minor >= 13), `node ${process.versions.node} is below engines >=22.13`)
})

// Peer packages are expected to be symlinked to the live harness root by
// `npm run setup:peer` (scripts/link-harness-dsh-tools.sh). Skip when they are
// not present — peers are never installed by `npm install` in CI shapes that
// use --legacy-peer-deps or omit them; the link script itself is the thing
// under test there, not this repo's unit surface.
test('T01 scaffold: harness peers resolve after setup:peer (skip-if-missing)', async (t) => {
  const peers = ['@deepseek-ai/cordis', '@deepseek-ai/dsh-tools', '@deepseek-ai/dsh-subagent']
  let missing
  try {
    for (const name of peers) import.meta.resolve(name)
  } catch (error) {
    missing = error
  }
  if (missing) return t.skip(`peers not linked (run npm run setup:peer): ${missing.code ?? missing.message}`)
  for (const name of peers) {
    const resolved = import.meta.resolve(name)
    assert.match(resolved, /@deepseek-ai\//, `${name} resolved outside the @deepseek-ai scope: ${resolved}`)
  }
})
