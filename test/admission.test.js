import { test } from 'node:test'
import assert from 'node:assert/strict'

import { createAdmission } from '../lib/admission.js'

// T06 + M6: in-memory resource admission (DESIGN §5.4). Slots = PER-RUN
// counting semaphore (M6 fix: each run's maxRunningAgents caps only its
// OWN pool — run A at capacity never starves run B). Session keys = strict
// mutex per spec `concurrencyKey` with owner-checked release.

test('admission: slot acquire below the bound returns a handle; heldCount tracks', () => {
  const a = createAdmission()
  assert.equal(a.heldCount(), 0)
  const h1 = a.tryAcquireSlot(2, 'run-a')
  assert.notEqual(h1, null)
  assert.equal(typeof h1.slot, 'number')
  assert.equal(a.heldCount(), 1)
  const h2 = a.tryAcquireSlot(2, 'run-a')
  assert.notEqual(h2, null)
  assert.equal(a.heldCount(), 2)
  // distinct slot indices within the run's pool
  assert.notEqual(h1.slot, h2.slot)
})

test('admission: slot acquire at capacity returns null (task stays ready)', () => {
  const a = createAdmission()
  assert.notEqual(a.tryAcquireSlot(1, 'run-a'), null)
  assert.equal(a.tryAcquireSlot(1, 'run-a'), null) // full → null, never an error
  assert.equal(a.heldCount(), 1)
})

test('admission: released slot is re-acquirable', () => {
  const a = createAdmission()
  const h1 = a.tryAcquireSlot(1, 'run-a')
  assert.notEqual(h1, null)
  assert.equal(a.tryAcquireSlot(1, 'run-a'), null)
  assert.equal(a.releaseSlot(h1), true)
  assert.equal(a.heldCount(), 0)
  const h2 = a.tryAcquireSlot(1, 'run-a')
  assert.notEqual(h2, null)
})

test('admission: M6 — two runs each max=2 never starve each other (per-run buckets)', () => {
  const a = createAdmission()
  // Run A takes its FULL quota of 2.
  const a1 = a.tryAcquireSlot(2, 'run-a')
  const a2 = a.tryAcquireSlot(2, 'run-a')
  assert.notEqual(a1, null)
  assert.notEqual(a2, null)
  assert.equal(a.tryAcquireSlot(2, 'run-a'), null) // A's own pool is full
  // Run B can STILL take its own 2 — the global-pool starvation bug (M1
  // review M6) would have refused both.
  const b1 = a.tryAcquireSlot(2, 'run-b')
  const b2 = a.tryAcquireSlot(2, 'run-b')
  assert.notEqual(b1, null, 'run B takes slot 1 while run A holds its full quota')
  assert.notEqual(b2, null, 'run B takes slot 2 while run A holds its full quota')
  assert.equal(a.tryAcquireSlot(2, 'run-b'), null) // B's own pool full now
  assert.equal(a.heldCount(), 4)
  assert.equal(a.heldCountForRun('run-a'), 2)
  assert.equal(a.heldCountForRun('run-b'), 2)
  // Distinct pools, distinct index spaces: both runs legitimately hold
  // "slot 0" of their own bucket.
  assert.equal(a1.slot, 0)
  assert.equal(b1.slot, 0)
  // Releasing A's slot frees only A's pool — B stays full.
  a.releaseSlot(a1)
  assert.equal(a.heldCountForRun('run-a'), 1)
  assert.equal(a.heldCountForRun('run-b'), 2)
  assert.equal(a.tryAcquireSlot(2, 'run-b'), null)
  assert.notEqual(a.tryAcquireSlot(2, 'run-a'), null) // A has room again
})

test('admission: M6 — a handle releases only its own run\'s pool (防误释)', () => {
  const a = createAdmission()
  const a1 = a.tryAcquireSlot(1, 'run-a')
  const b1 = a.tryAcquireSlot(1, 'run-b')
  assert.notEqual(a1, null)
  assert.notEqual(b1, null)
  // Releasing run A's handle leaves run B's pool untouched...
  assert.equal(a.releaseSlot(a1), true)
  assert.equal(a.heldCountForRun('run-b'), 1)
  assert.equal(a.tryAcquireSlot(1, 'run-b'), null)
  // ...and run A can re-acquire while B still holds its own.
  assert.notEqual(a.tryAcquireSlot(1, 'run-a'), null)
})

test('admission: the dynamic bound can differ per call (engine passes per-run limits)', () => {
  const a = createAdmission()
  // Same run, different bounds: the bucket is shared within the run.
  a.tryAcquireSlot(2, 'run-a')
  a.tryAcquireSlot(2, 'run-a')
  assert.equal(a.tryAcquireSlot(2, 'run-a'), null) // bound 2 is full
  const h3 = a.tryAcquireSlot(3, 'run-a')
  assert.notEqual(h3, null) // bound 3 has room in the SAME pool
  assert.equal(a.heldCount(), 3)
  assert.equal(a.tryAcquireSlot(3, 'run-a'), null) // now bound 3 is full too
  a.releaseSlot(h3)
  assert.equal(a.heldCount(), 2)
})

test('admission: releaseSlot is idempotent for un-held slots (lost race is a no-op)', () => {
  const a = createAdmission()
  const h = a.tryAcquireSlot(1, 'run-a')
  assert.equal(a.releaseSlot(h), true)
  assert.equal(a.releaseSlot(h), false) // double release: false, no throw
  assert.equal(a.releaseSlot(99), false) // never-acquired slot: false
})

test('admission: session key is exclusive while held', () => {
  const a = createAdmission()
  assert.equal(a.tryAcquireSessionKey('db-primary', 'att-1'), true)
  assert.equal(a.isSessionKeyHeld('db-primary'), true)
  // Same key: a DIFFERENT attempt is refused...
  assert.equal(a.tryAcquireSessionKey('db-primary', 'att-2'), false)
  // ...and even the current holder re-acquiring is refused (strict mutex).
  assert.equal(a.tryAcquireSessionKey('db-primary', 'att-1'), false)
})

test('admission: session key release frees the mutex for the next attempt', () => {
  const a = createAdmission()
  assert.equal(a.tryAcquireSessionKey('k', 'att-1'), true)
  assert.equal(a.releaseSessionKey('k', 'att-1'), true)
  assert.equal(a.isSessionKeyHeld('k'), false)
  assert.equal(a.tryAcquireSessionKey('k', 'att-2'), true)
})

test('admission: releasing a session key with the wrong owner is a no-op (防误释)', () => {
  const a = createAdmission()
  assert.equal(a.tryAcquireSessionKey('k', 'att-1'), true)
  assert.equal(a.releaseSessionKey('k', 'att-2'), false) // wrong owner
  assert.equal(a.isSessionKeyHeld('k'), true) // still held by att-1
  assert.equal(a.tryAcquireSessionKey('k', 'att-2'), false) // still exclusive
  assert.equal(a.releaseSessionKey('k', 'att-1'), true) // right owner works
})

test('admission: different session keys coexist independently', () => {
  const a = createAdmission()
  assert.equal(a.tryAcquireSessionKey('k1', 'att-1'), true)
  assert.equal(a.tryAcquireSessionKey('k2', 'att-2'), true)
  assert.equal(a.isSessionKeyHeld('k1'), true)
  assert.equal(a.isSessionKeyHeld('k2'), true)
  assert.equal(a.tryAcquireSessionKey('k1', 'att-3'), false) // k1 exclusive
  assert.equal(a.tryAcquireSessionKey('k3', 'att-4'), true) // k3 free
})

test('admission: slots and session keys are independent gates', () => {
  const a = createAdmission()
  const h = a.tryAcquireSlot(1, 'run-a')
  assert.notEqual(h, null)
  assert.equal(a.tryAcquireSlot(1, 'run-a'), null) // slots exhausted
  assert.equal(a.tryAcquireSessionKey('k', 'att-1'), true) // but keys still work
  assert.equal(a.isSessionKeyHeld('k'), true)
})

test('admission: malformed arguments throw loud TypeErrors (programming errors)', () => {
  const a = createAdmission()
  assert.throws(() => a.tryAcquireSlot(0, 'run-a'), TypeError)
  assert.throws(() => a.tryAcquireSlot(-1, 'run-a'), TypeError)
  assert.throws(() => a.tryAcquireSlot(1.5, 'run-a'), TypeError)
  assert.throws(() => a.tryAcquireSlot('2', 'run-a'), TypeError)
  // runId is required per M6 (a missing run is a programming error).
  assert.throws(() => a.tryAcquireSlot(2), TypeError)
  assert.throws(() => a.tryAcquireSlot(2, ''), TypeError)
  assert.throws(() => a.tryAcquireSlot(2, 42), TypeError)
  assert.throws(() => a.tryAcquireSessionKey('', 'att-1'), TypeError)
  assert.throws(() => a.tryAcquireSessionKey('k', ''), TypeError)
  assert.throws(() => a.releaseSessionKey('k', null), TypeError)
  assert.throws(() => a.isSessionKeyHeld(42), TypeError)
  assert.throws(() => a.heldCountForRun(''), TypeError)
})

test('admission: lowest free slot index is reused deterministically (within one run)', () => {
  const a = createAdmission()
  const h1 = a.tryAcquireSlot(3, 'run-a') // slot 0
  const h2 = a.tryAcquireSlot(3, 'run-a') // slot 1
  assert.equal(h1.slot, 0)
  assert.equal(h2.slot, 1)
  a.releaseSlot(h1)
  const h3 = a.tryAcquireSlot(3, 'run-a')
  assert.equal(h3.slot, 0) // lowest free index reused
})

test('admission: empty buckets are dropped (no unbounded Map growth)', () => {
  const a = createAdmission()
  const h = a.tryAcquireSlot(1, 'run-x')
  a.releaseSlot(h)
  assert.equal(a.heldCountForRun('run-x'), 0)
  assert.equal(a.heldCount(), 0)
})
