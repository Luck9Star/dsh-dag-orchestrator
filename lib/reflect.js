/**
 * Promise status reflection (DESIGN §4.4 D8 / §5.2).
 *
 * New code (not ported): the engine's `inFlight` map holds the authoritative
 * `SubagentRun.result` promises; `harvestSettled()` needs a SYNCHRONOUS
 * "has this settled yet?" probe each tick, and `dag_tick`'s bounded wait needs
 * a never-rejecting wrapper it can Promise.race on.
 *
 * Family discipline (per DESIGN §4.4, dsh-worktrees §2.2 lineage): no
 * `Promise.withResolvers` — Node ≥22 family rule. Implementation is the
 * classic closure-flag wrapper: attach a then-callback to the promise (does
 * not affect the original chain), record settlement in a WeakMap-registered
 * record.
 *
 * Both exports share one WeakMap registry, so `reflect(p)` and
 * `promiseSettledSync(p)` agree on the same underlying promise, and repeated
 * calls on the same promise return consistent results (the wrapper never
 * swallows the original: attaching `.then` creates a new derived promise and
 * leaves `p`'s own state/reactions untouched).
 */

/** @typedef {'pending' | 'fulfilled'} SettledStatus */

/**
 * @typedef {object} ReflectedState
 * @property {SettledStatus} status
 * @property {unknown} [value]   Present when the promise fulfilled.
 * @property {unknown} [reason]  Present when the promise rejected.
 */

/**
 * Shared registry: promise → mutable settlement record (the flag bit lives in
 * `record.status`). Weak so tracking a promise never pins it (or anything it
 * closes over) for the run lifetime. The record is written exactly once (by
 * the attached then-callback).
 *
 * @type {WeakMap<Promise<unknown>, ReflectedState & { value?: unknown, reason?: unknown }>}
 */
const registry = new WeakMap();

/**
 * Register `p` in the settlement registry (idempotent — already-tracked
 * promises return their existing record). Attaching the then-callback has no
 * effect on `p` itself or on callbacks other callers attached.
 *
 * @template T
 * @param {Promise<T>} p
 */
function track(p) {
  let record = registry.get(p);
  if (record) return record;
  record = { status: 'pending' };
  registry.set(p, record);
  Promise.resolve(p).then(
    (value) => {
      record.status = 'fulfilled';
      record.value = value;
    },
    (reason) => {
      record.status = 'fulfilled';
      record.reason = reason;
    },
  );
  return record;
}

/**
 * Synchronous settlement probe. Returns 'pending' until the underlying
 * promise settles (fulfil OR rejection both count — this is the harvest
 * gate), 'fulfilled' after. Registers the promise on first contact.
 *
 * @template T
 * @param {Promise<T>} p
 * @returns {SettledStatus}
 */
export function promiseSettledSync(p) {
  return track(p).status;
}

/**
 * Never-rejecting status wrapper: resolves with `{status, value?, reason?}`
 * once the underlying promise settles; while pending, the returned promise is
 * itself pending (safe to race in boundedRace). The snapshot shape mirrors
 * Promise.allSettled's element shape, narrowed to pending/fulfilled.
 *
 * @template T
 * @param {Promise<T>} p
 * @returns {Promise<ReflectedState>}
 */
export function reflect(p) {
  track(p); // register in the shared registry so promiseSettledSync(p) agrees
  return Promise.resolve(p).then(
    (value) => ({ status: 'fulfilled', value }),
    (reason) => ({ status: 'fulfilled', reason }),
  );
}
