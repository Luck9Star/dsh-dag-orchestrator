/**
 * graph.ts tests (jsdom): layered DAG layout, cycle errors, orthogonal
 * polylines, and deterministic placement from SpecView fixtures.
 */
import { describe, expect, it } from 'vitest'
import { computeLayout } from '../../src/core/graph.ts'
import type { SpecTask, SpecView, TaskKind } from '../../src/core/types.ts'

function task(id: string, dependsOn: string[] = [], kind: TaskKind = 'agent'): SpecTask {
  return {
    id,
    kind,
    dependsOn: dependsOn.map((taskId) => ({ taskId, condition: 'succeeded' })),
  }
}

function spec(tasks: SpecTask[], name = 'fixture'): SpecView {
  return { version: 1, name, tasks }
}

function byId(nodes: ReturnType<typeof computeLayout>['nodes']): Map<string, (typeof nodes)[number]> {
  return new Map(nodes.map((n) => [n.id, n]))
}

describe('computeLayout', () => {
  it('places a chain a→b→c on layers 0/1/2 with increasing x', () => {
    const layout = computeLayout(spec([
      task('a'),
      task('b', ['a']),
      task('c', ['b']),
    ]))
    const nodes = byId(layout.nodes)
    expect(nodes.get('a')!.layer).toBe(0)
    expect(nodes.get('b')!.layer).toBe(1)
    expect(nodes.get('c')!.layer).toBe(2)
    expect(nodes.get('a')!.x).toBeLessThan(nodes.get('b')!.x)
    expect(nodes.get('b')!.x).toBeLessThan(nodes.get('c')!.x)
    expect(layout.edges.map((e) => `${e.from}->${e.to}`).sort()).toEqual(['a->b', 'b->c'])
  })

  it('places diamond a→{b,c}→d with b and c on the same layer, ordered by id', () => {
    const layout = computeLayout(spec([
      task('d', ['c', 'b']),
      task('a'),
      task('c', ['a']),
      task('b', ['a']),
    ]))
    const nodes = byId(layout.nodes)
    expect(nodes.get('a')!.layer).toBe(0)
    expect(nodes.get('b')!.layer).toBe(1)
    expect(nodes.get('c')!.layer).toBe(1)
    expect(nodes.get('d')!.layer).toBe(2)
    const mid = layout.nodes.filter((n) => n.layer === 1)
    expect(mid.map((n) => n.id)).toEqual(['b', 'c'])
    expect(mid[0]!.y).toBeLessThan(mid[1]!.y)
    expect(mid[1]!.y - mid[0]!.y).toBe(mid[0]!.h + 16)
  })

  it('puts an isolated node (no deps, no dependents) on layer 0', () => {
    const layout = computeLayout(spec([
      task('solo'),
      task('a'),
      task('b', ['a']),
    ]))
    const nodes = byId(layout.nodes)
    expect(nodes.get('solo')!.layer).toBe(0)
    expect(nodes.get('a')!.layer).toBe(0)
    expect(layout.edges.every((e) => e.from !== 'solo' && e.to !== 'solo')).toBe(true)
  })

  it('throws on a cycle a→b→a, naming both ids', () => {
    expect(() => computeLayout(spec([
      task('a', ['b']),
      task('b', ['a']),
    ]))).toThrow(/cycle/i)
    try {
      computeLayout(spec([task('a', ['b']), task('b', ['a'])]))
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      expect(message).toMatch(/a/)
      expect(message).toMatch(/b/)
    }
  })

  it('draws an orthogonal polyline from a bottom-center to b top-center', () => {
    const layout = computeLayout(spec([task('a'), task('b', ['a'])]))
    const nodes = byId(layout.nodes)
    const a = nodes.get('a')!
    const b = nodes.get('b')!
    const edge = layout.edges.find((e) => e.from === 'a' && e.to === 'b')
    expect(edge).toBeDefined()
    const points = edge!.points
    expect(points).toHaveLength(4)
    expect(points[0]).toEqual({ x: a.x + a.w / 2, y: a.y + a.h })
    expect(points[points.length - 1]).toEqual({ x: b.x + b.w / 2, y: b.y })
    const midY = points[1]!.y
    expect(points[1]!.y).toBe(midY)
    expect(points[2]!.y).toBe(midY)
    expect(points[1]!.x).toBe(points[0]!.x)
    expect(points[2]!.x).toBe(points[3]!.x)
  })

  it('is deterministic across two calls with an identical spec', () => {
    const fixture = spec([
      task('d', ['b', 'c']),
      task('c', ['a']),
      task('b', ['a']),
      task('a'),
      task('z'),
    ])
    const first = computeLayout(fixture)
    const second = computeLayout(fixture)
    expect(JSON.stringify(second)).toBe(JSON.stringify(first))
  })
})
