import { test } from 'node:test'
import assert from 'node:assert/strict'

import { BoundedQueue, compareQueueEntries } from '../lib/bounded-queue.js'

// Ported from task-weaver packages/scheduler/src/__tests__/bounded-queue.test.ts
// (L1-161, bun:test → node:test) plus direct comparator-table cases. Migrated
// cases keep the source expectations verbatim — no weakening.

/**
 * @param {string} taskId
 * @param {{ humanUnblocked?: boolean, criticalPathDepth?: number, priority?: number, readyAt?: number }} [opts]
 */
function entry(taskId, opts = {}) {
  return {
    taskId,
    humanUnblocked: opts.humanUnblocked ?? false,
    criticalPathDepth: opts.criticalPathDepth ?? 0,
    priority: opts.priority ?? 0,
    readyAt: opts.readyAt ?? 0,
  }
}

// --- compareQueueEntries: the 5-level table, one level per case -------------

test('comparator level 1: humanUnblocked true ranks first regardless of other fields', () => {
  const unblocked = entry('unblocked', { humanUnblocked: true, criticalPathDepth: 0, priority: 0 })
  const deep = entry('deep', { humanUnblocked: false, criticalPathDepth: 99, priority: 99 })
  assert.equal(compareQueueEntries(unblocked, deep), -1)
  assert.equal(compareQueueEntries(deep, unblocked), 1)
})

test('comparator level 2: higher criticalPathDepth ranks earlier (descending)', () => {
  const deep = entry('deep', { criticalPathDepth: 5 })
  const mid = entry('mid', { criticalPathDepth: 3 })
  const shallow = entry('shallow', { criticalPathDepth: 1 })
  assert.equal(compareQueueEntries(deep, mid), -2) // 3 - 5
  assert.equal(compareQueueEntries(mid, shallow), -2) // 1 - 3
  assert.equal(compareQueueEntries(shallow, deep), 4) // 5 - 1
})

test('comparator level 3: higher explicit priority ranks earlier (descending)', () => {
  const hi = entry('hi', { criticalPathDepth: 2, priority: 9 })
  const mid = entry('mid', { criticalPathDepth: 2, priority: 5 })
  const lo = entry('lo', { criticalPathDepth: 2, priority: 1 })
  assert.equal(compareQueueEntries(hi, mid), -4) // 5 - 9
  assert.equal(compareQueueEntries(mid, lo), -4)
  assert.equal(compareQueueEntries(lo, hi), 8)
})

test('comparator level 4: earlier readyAt ranks earlier (ascending FIFO)', () => {
  const early = entry('early', { priority: 1, readyAt: 10 })
  const mid = entry('mid', { priority: 1, readyAt: 50 })
  const late = entry('late', { priority: 1, readyAt: 100 })
  assert.equal(compareQueueEntries(early, mid), -40)
  assert.equal(compareQueueEntries(mid, late), -50)
  assert.equal(compareQueueEntries(late, early), 90)
})

test('comparator level 5: full tie breaks by taskId lexicographic ascending', () => {
  const alpha = entry('alpha')
  const bravo = entry('bravo')
  const charlie = entry('charlie')
  assert.equal(compareQueueEntries(alpha, bravo), -1)
  assert.equal(compareQueueEntries(bravo, alpha), 1)
  assert.equal(compareQueueEntries(alpha, alpha), 0)
  assert.ok(compareQueueEntries(alpha, charlie) < 0)
  assert.ok(compareQueueEntries(charlie, bravo) > 0)
})

test('comparator: levels are strictly lexicographic (level 1 decision never overridden)', () => {
  // A lower level can NEVER flip a higher level's decision: same humanUnblocked
  // and depth, but priority difference must not beat a humanUnblocked
  // difference when comparing across groups.
  const a = entry('a', { humanUnblocked: true, criticalPathDepth: 1, priority: 0, readyAt: 999 })
  const b = entry('b', { humanUnblocked: false, criticalPathDepth: 9, priority: 100, readyAt: 1 })
  assert.equal(compareQueueEntries(a, b), -1)
  assert.equal(compareQueueEntries(b, a), 1)
})

// --- BoundedQueue — capacity enforcement (migrated source cases) -------------

test('BoundedQueue: enqueues up to capacity entries, then rejects without throwing', () => {
  const q = new BoundedQueue(3)
  assert.equal(q.capacity, 3)

  assert.equal(q.tryEnqueue(entry('a')), true)
  assert.equal(q.tryEnqueue(entry('b')), true)
  assert.equal(q.tryEnqueue(entry('c')), true)

  assert.equal(q.tryEnqueue(entry('d')), false)
  assert.equal(q.size, 3)
})

test('BoundedQueue: tryEnqueue never throws on overflow (scheduler re-evaluates next tick)', () => {
  const q = new BoundedQueue(1)
  q.tryEnqueue(entry('a'))
  assert.doesNotThrow(() => q.tryEnqueue(entry('b')))
  assert.equal(q.size, 1)
})

test('BoundedQueue: drain empties the queue and returns entries in deterministic priority order', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('c', { priority: 1 }))
  q.tryEnqueue(entry('a', { priority: 9 }))
  q.tryEnqueue(entry('b', { priority: 5 }))

  const drained = q.drain()
  assert.deepEqual(
    drained.map((e) => e.taskId),
    ['a', 'b', 'c'],
  )
  assert.equal(q.size, 0)
  assert.equal(q.isEmpty, true)
  assert.deepEqual(q.drain(), [])
})

test('BoundedQueue: peek returns the next entry by priority without removing it', () => {
  const q = new BoundedQueue(4)
  q.tryEnqueue(entry('low', { priority: 1 }))
  q.tryEnqueue(entry('high', { priority: 9 }))
  assert.equal(q.peek()?.taskId, 'high')
  assert.equal(q.size, 2)
  assert.equal(q.peek()?.taskId, 'high') // still there — peek does not mutate
  assert.deepEqual(
    q.drain().map((e) => e.taskId),
    ['high', 'low'],
  )
})

test('BoundedQueue: peek returns undefined on an empty queue', () => {
  const q = new BoundedQueue(4)
  assert.equal(q.peek(), undefined)
})

test('BoundedQueue: isEmpty reflects emptiness', () => {
  const q = new BoundedQueue(2)
  assert.equal(q.isEmpty, true)
  q.tryEnqueue(entry('a'))
  assert.equal(q.isEmpty, false)
  q.drain()
  assert.equal(q.isEmpty, true)
})

// --- BoundedQueue — deterministic priority ordering (migrated source cases) --

test('BoundedQueue ordering: human-unblocked tasks rank above the rest regardless of other fields', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('deep', { humanUnblocked: false, criticalPathDepth: 99, priority: 99 }))
  q.tryEnqueue(entry('unblocked', { humanUnblocked: true, criticalPathDepth: 0, priority: 0 }))

  assert.deepEqual(
    q.drain().map((e) => e.taskId),
    ['unblocked', 'deep'],
  )
})

test('BoundedQueue ordering: among same humanUnblocked, higher critical-path-depth wins', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('shallow', { criticalPathDepth: 1 }))
  q.tryEnqueue(entry('deep', { criticalPathDepth: 5 }))
  q.tryEnqueue(entry('mid', { criticalPathDepth: 3 }))

  assert.deepEqual(
    q.drain().map((e) => e.taskId),
    ['deep', 'mid', 'shallow'],
  )
})

test('BoundedQueue ordering: among same depth, higher explicit priority wins', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('lo', { criticalPathDepth: 2, priority: 1 }))
  q.tryEnqueue(entry('hi', { criticalPathDepth: 2, priority: 9 }))
  q.tryEnqueue(entry('mid', { criticalPathDepth: 2, priority: 5 }))

  assert.deepEqual(
    q.drain().map((e) => e.taskId),
    ['hi', 'mid', 'lo'],
  )
})

test('BoundedQueue ordering: among same priority, earlier ready-time wins (FIFO tiebreak)', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('late', { priority: 1, readyAt: 100 }))
  q.tryEnqueue(entry('early', { priority: 1, readyAt: 10 }))
  q.tryEnqueue(entry('mid', { priority: 1, readyAt: 50 }))

  assert.deepEqual(
    q.drain().map((e) => e.taskId),
    ['early', 'mid', 'late'],
  )
})

test('BoundedQueue ordering: full tie → task_id lexicographic stable sort', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('charlie'))
  q.tryEnqueue(entry('alpha'))
  q.tryEnqueue(entry('bravo'))

  assert.deepEqual(
    q.drain().map((e) => e.taskId),
    ['alpha', 'bravo', 'charlie'],
  )
})

test('BoundedQueue ordering: deterministic — same inputs always produce the same drain order', () => {
  function buildAndDrain() {
    const q = new BoundedQueue(16)
    q.tryEnqueue(entry('t1', { humanUnblocked: false, criticalPathDepth: 2, priority: 1, readyAt: 5 }))
    q.tryEnqueue(entry('t2', { humanUnblocked: true, criticalPathDepth: 1, priority: 0, readyAt: 9 }))
    q.tryEnqueue(entry('t3', { humanUnblocked: false, criticalPathDepth: 2, priority: 1, readyAt: 5 }))
    q.tryEnqueue(entry('t4', { humanUnblocked: false, criticalPathDepth: 4, priority: 2, readyAt: 1 }))
    return q.drain().map((e) => e.taskId)
  }
  const a = buildAndDrain()
  const b = buildAndDrain()
  assert.deepEqual(a, b)
  assert.deepEqual(a, ['t2', 't4', 't1', 't3'])
})

// --- BoundedQueue — remove (migrated source cases) --------------------------

test('BoundedQueue: remove pulls a specific entry out of the queue by taskId', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('a'))
  q.tryEnqueue(entry('b'))
  q.tryEnqueue(entry('c'))

  assert.equal(q.remove('b'), true)
  assert.equal(q.size, 2)
  assert.deepEqual(
    q.drain().map((e) => e.taskId),
    ['a', 'c'],
  )
})

test('BoundedQueue: remove returns false for a taskId not in the queue', () => {
  const q = new BoundedQueue(8)
  q.tryEnqueue(entry('a'))
  assert.equal(q.remove('zzz'), false)
  assert.equal(q.size, 1)
})

test('BoundedQueue: remove frees a slot at capacity', () => {
  const q = new BoundedQueue(1)
  q.tryEnqueue(entry('a'))
  assert.equal(q.tryEnqueue(entry('b')), false)
  assert.equal(q.remove('a'), true)
  assert.equal(q.tryEnqueue(entry('b')), true)
  assert.equal(q.size, 1)
})

// --- BoundedQueue — constructor validation ----------------------------------

test('BoundedQueue: constructor rejects non-positive-integer capacity', () => {
  for (const bad of [0, -1, 1.5, NaN, Infinity, '3', null]) {
    assert.throws(() => new BoundedQueue(bad), /queueCapacity must be a positive integer/)
  }
})

test('BoundedQueue: constructor accepts positive integers', () => {
  assert.doesNotThrow(() => new BoundedQueue(1))
  assert.doesNotThrow(() => new BoundedQueue(1024))
  assert.equal(new BoundedQueue(1024).capacity, 1024)
})
