/**
 * mount tests (jsdom): createMount starts the observer and injects the
 * placeholder into a simulated sidebar container; stop() cleans up.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { apply } from '../../src/client/index.ts'
import {
  ACTIVE_ATTRIBUTE, ENTRY_SELECTOR, PANEL_SELECTOR, createMount, isPanelActive, togglePanel,
} from '../../src/client/mount.tsx'

afterEach(() => {
  document.documentElement.removeAttribute(ACTIVE_ATTRIBUTE)
  document.body.innerHTML = ''
})

describe('createMount', () => {
  it('injects the sidebar entry once the sidebar pane appears', async () => {
    const mount = createMount()
    mount.start()
    // The sidebar shell is not there yet; nothing is injected.
    expect(document.querySelector(ENTRY_SELECTOR)).toBeNull()

    // Simulate the shell rendering after boot.
    const sidebar = document.createElement('div')
    sidebar.dataset.pane = 'sidebar'
    document.body.appendChild(sidebar)
    // MutationObserver fires async; flush microtasks.
    await new Promise((resolve) => { setTimeout(resolve, 0) })

    const entry = document.querySelector<HTMLButtonElement>(ENTRY_SELECTOR)
    expect(entry).not.toBeNull()
    expect(sidebar.contains(entry)).toBe(true)

    mount.stop()
    expect(document.querySelector(ENTRY_SELECTOR)).toBeNull()
  })

  it('mounts the placeholder center panel and cleans up on stop', () => {
    const conversation = document.createElement('div')
    conversation.dataset.pane = 'conversation'
    document.body.appendChild(conversation)

    const mount = createMount()
    mount.start()
    const panel = document.querySelector(PANEL_SELECTOR)
    expect(panel).not.toBeNull()
    expect(conversation.contains(panel)).toBe(true)

    mount.stop()
    expect(document.querySelector(PANEL_SELECTOR)).toBeNull()
    expect(mount.started).toBe(false)
  })

  it('re-inserts the entry after a re-render drops it', async () => {
    const mount = createMount()
    mount.start()
    const sidebar = document.createElement('div')
    sidebar.dataset.pane = 'sidebar'
    document.body.appendChild(sidebar)
    await new Promise((resolve) => { setTimeout(resolve, 0) })
    const entry = document.querySelector<HTMLButtonElement>(ENTRY_SELECTOR)
    expect(entry).not.toBeNull()

    // A React re-render displaces the row.
    entry!.remove()
    await new Promise((resolve) => { setTimeout(resolve, 0) })
    expect(document.querySelector(ENTRY_SELECTOR)).not.toBeNull()

    mount.stop()
  })

  it('toggles the panel visibility through the html attribute', () => {
    expect(isPanelActive()).toBe(false)
    expect(togglePanel()).toBe(true)
    expect(isPanelActive()).toBe(true)
    expect(togglePanel()).toBe(false)
    expect(isPanelActive()).toBe(false)
  })

  it('start is idempotent', () => {
    const mount = createMount()
    mount.start()
    mount.start()
    expect(mount.started).toBe(true)
    mount.stop()
    expect(mount.started).toBe(false)
  })
})

describe('client entry apply', () => {
  it('starts and stops the mount through ctx.effect', () => {
    const effects: Array<() => (() => void) | undefined | void> = []
    const ctx = {
      effect: vi.fn((callback: () => (() => void) | undefined | void) => {
        effects.push(callback)
        return () => { }
      }),
    }
    apply(ctx)
    expect(ctx.effect).toHaveBeenCalledTimes(1)

    // Run the effect body: the mount starts (panel injected).
    const dispose = effects[0]!()
    expect(document.querySelector(PANEL_SELECTOR)).not.toBeNull()

    // Dispose: the mount stops and the DOM is cleaned.
    ;(dispose as () => void)()
    expect(document.querySelector(PANEL_SELECTOR)).toBeNull()
  })
})
