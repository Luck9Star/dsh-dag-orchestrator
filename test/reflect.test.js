import { test } from 'node:test'
import assert from 'node:assert/strict'

import { reflect, promiseSettledSync } from '../lib/reflect.js'

// New module (DESIGN §4.4 D8): promise status wrapper backing harvestSettled
// (sync probe) and boundedRace (never-reject wrapper). No Promise.withResolvers
// (Node ≥22 family discipline). All six semantics from the T02 brief.

const tick = () => new Promise((resolve) => setImmediate(resolve))

test('reflect: promiseSettledSync reports pending before settlement', async () => {
  let settle
  const p = new Promise((resolve) => { settle = resolve })
  assert.equal(promiseSettledSync(p), 'pending')
  settle(1)
  await tick() // let the tracking callback run
  assert.equal(promiseSettledSync(p), 'fulfilled')
})

test('reflect: fulfilled promise → status fulfilled with correct value', async () => {
  const p = Promise.resolve(42)
  assert.equal(promiseSettledSync(p), 'pending') // microtask not yet drained
  await p.catch(() => {})
  await tick()
  assert.equal(promiseSettledSync(p), 'fulfilled')
})

test('reflect: rejected promise → status fulfilled (not rejected) with reason captured', async () => {
  const boom = new Error('boom')
  const p = Promise.reject(boom)
  p.catch(() => {}) // silence Node's unhandled-rejection for the original
  assert.equal(promiseSettledSync(p), 'pending')
  await tick()
  assert.equal(promiseSettledSync(p), 'fulfilled')
  const snapshot = await reflect(p)
  assert.equal(snapshot.status, 'fulfilled')
  assert.equal(snapshot.reason, boom)
  assert.equal('value' in snapshot, false)
})

test('reflect: reflect() itself never rejects (fulfilled and rejected inputs)', async () => {
  const ok = await reflect(Promise.resolve('v'))
  assert.deepEqual(ok, { status: 'fulfilled', value: 'v' })

  const err = new Error('nope')
  const bad = await reflect(Promise.reject(err))
  assert.equal(bad.status, 'fulfilled')
  assert.equal(bad.reason, err)
})

test('reflect: repeated queries on the same promise stay consistent', async () => {
  let settle
  const p = new Promise((resolve) => { settle = resolve })
  const before = promiseSettledSync(p)
  const before2 = promiseSettledSync(p)
  assert.equal(before, 'pending')
  assert.equal(before2, 'pending')

  settle('x')
  await tick()
  const after = promiseSettledSync(p)
  const after2 = promiseSettledSync(p)
  assert.equal(after, 'fulfilled')
  assert.equal(after2, 'fulfilled')

  const r1 = await reflect(p)
  const r2 = await reflect(p)
  assert.deepEqual(r1, r2)
  assert.deepEqual(r1, { status: 'fulfilled', value: 'x' })
})

test('reflect: reflect and promiseSettledSync agree on the same promise (both outcomes)', async () => {
  // fulfilled — register FIRST (engine registers at dispatch time), let the
  // tracking callback run, then both views must agree.
  const f = Promise.resolve('fv')
  promiseSettledSync(f)
  await tick()
  assert.equal(promiseSettledSync(f), 'fulfilled')
  assert.equal((await reflect(f)).status, 'fulfilled')

  // rejected
  const r = Promise.reject(new Error('rv'))
  r.catch(() => {})
  promiseSettledSync(r)
  await tick()
  assert.equal(promiseSettledSync(r), 'fulfilled')
  assert.equal((await reflect(r)).status, 'fulfilled')

  // still pending
  const pend = new Promise(() => {})
  promiseSettledSync(pend)
  assert.equal(promiseSettledSync(pend), 'pending')
  let reflected = false
  reflect(pend).then(() => { reflected = true })
  await tick()
  assert.equal(reflected, false) // reflect(pending) stays pending — raceable
})

test('reflect: wrapper does not swallow the original promise (other consumers still see it)', async () => {
  let resolveOriginal
  const p = new Promise((resolve) => { resolveOriginal = resolve })

  const reflected = reflect(p)
  promiseSettledSync(p) // shared registry path

  resolveOriginal('payload')
  // The ORIGINAL promise still delivers its value to its own consumers.
  assert.equal(await p, 'payload')
  // And the wrapper resolved with the same value.
  assert.deepEqual(await reflected, { status: 'fulfilled', value: 'payload' })
  assert.equal(promiseSettledSync(p), 'fulfilled')
})

test('reflect: tracking a pending promise does not keep it alive (WeakMap registry)', async () => {
  // WeakMap semantics: once the caller drops the last reference, the record is
  // collectable. We can only assert the observable contract — registration has
  // no side effects on the promise and no leak-facing API exists.
  let resolveIt
  const p = new Promise((resolve) => { resolveIt = resolve })
  assert.equal(promiseSettledSync(p), 'pending')
  resolveIt(undefined)
  await tick()
  assert.equal(promiseSettledSync(p), 'fulfilled')
})
