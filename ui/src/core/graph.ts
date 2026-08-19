/**
 * Pure DAG layout for SpecView. No DOM — layered longest-path placement
 * plus orthogonal edge polylines. Unknown dependsOn targets are ignored.
 * @module dsh-dag-view/core/graph
 */

import type { SpecView } from './types.ts'

export interface LayoutNode {
  id: string
  kind: string
  layer: number
  x: number
  y: number
  w: number
  h: number
}

export interface LayoutEdge {
  from: string
  to: string
  points: Array<{ x: number; y: number }>
}

export interface Layout {
  nodes: LayoutNode[]
  edges: LayoutEdge[]
}

const NODE_W = 180
const NODE_H = 44
const GAP_X = 60
const GAP_Y = 16
const LONG_ID = 18

function nodeHeight(id: string): number {
  return id.length <= LONG_ID ? NODE_H : NODE_H + Math.ceil((id.length - LONG_ID) / 10) * 12
}

function compareId(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0
}

/** DFS cycle path (including the repeated start), or null if acyclic. */
function findCycle(ids: readonly string[], adj: Map<string, string[]>): string[] | null {
  const state = new Map<string, 'visiting' | 'done'>()
  const stack: string[] = []

  const dfs = (u: string): string[] | null => {
    state.set(u, 'visiting')
    stack.push(u)
    for (const v of adj.get(u) ?? []) {
      if (state.get(v) === 'visiting') {
        return [...stack.slice(stack.indexOf(v)), v]
      }
      if (state.get(v) !== 'done') {
        const found = dfs(v)
        if (found) return found
      }
    }
    stack.pop()
    state.set(u, 'done')
    return null
  }

  for (const id of ids) {
    if (!state.has(id)) {
      const found = dfs(id)
      if (found) return found
    }
  }
  return null
}

function longestPathLayers(ids: readonly string[], preds: Map<string, string[]>): Map<string, number> {
  const memo = new Map<string, number>()

  const depth = (id: string): number => {
    const cached = memo.get(id)
    if (cached !== undefined) return cached
    const incoming = preds.get(id) ?? []
    const layer = incoming.length === 0 ? 0 : Math.max(...incoming.map(depth)) + 1
    memo.set(id, layer)
    return layer
  }

  for (const id of ids) depth(id)
  return memo
}

export function computeLayout(spec: SpecView): Layout {
  const tasks = spec.tasks
  const byId = new Map(tasks.map((t) => [t.id, t]))
  const ids = [...byId.keys()].sort(compareId)

  const succs = new Map<string, string[]>()
  const preds = new Map<string, string[]>()
  const edgeKeys: Array<{ from: string; to: string }> = []
  const seen = new Set<string>()

  for (const id of ids) {
    succs.set(id, [])
    preds.set(id, [])
  }

  for (const task of tasks) {
    for (const dep of task.dependsOn) {
      if (!byId.has(dep.taskId)) continue
      const key = `${dep.taskId}\0${task.id}`
      if (seen.has(key)) continue
      seen.add(key)
      succs.get(dep.taskId)!.push(task.id)
      preds.get(task.id)!.push(dep.taskId)
      edgeKeys.push({ from: dep.taskId, to: task.id })
    }
  }

  for (const id of ids) {
    succs.get(id)!.sort(compareId)
    preds.get(id)!.sort(compareId)
  }

  const cycle = findCycle(ids, succs)
  if (cycle) {
    throw new Error(`cycle detected: ${cycle.join(' -> ')}`)
  }

  const layers = longestPathLayers(ids, preds)

  const byLayer = new Map<number, string[]>()
  for (const id of ids) {
    const layer = layers.get(id) ?? 0
    const bucket = byLayer.get(layer)
    if (bucket) bucket.push(id)
    else byLayer.set(layer, [id])
  }
  for (const bucket of byLayer.values()) bucket.sort(compareId)

  const placed = new Map<string, LayoutNode>()
  const nodes: LayoutNode[] = []
  const layerKeys = [...byLayer.keys()].sort((a, b) => a - b)
  for (const layer of layerKeys) {
    const bucket = byLayer.get(layer) ?? []
    let y = 0
    for (const id of bucket) {
      const task = byId.get(id)!
      const h = nodeHeight(id)
      const node: LayoutNode = {
        id,
        kind: task.kind ?? 'agent',
        layer,
        x: layer * (NODE_W + GAP_X),
        y,
        w: NODE_W,
        h,
      }
      placed.set(id, node)
      nodes.push(node)
      y += h + GAP_Y
    }
  }

  edgeKeys.sort((a, b) => compareId(a.from, b.from) || compareId(a.to, b.to))
  const edges: LayoutEdge[] = []
  for (const { from, to } of edgeKeys) {
    const source = placed.get(from)
    const target = placed.get(to)
    if (!source || !target) continue
    const sx = source.x + source.w / 2
    const tx = target.x + target.w / 2
    const midY = (source.y + source.h + target.y) / 2
    edges.push({
      from,
      to,
      points: [
        { x: sx, y: source.y + source.h },
        { x: sx, y: midY },
        { x: tx, y: midY },
        { x: tx, y: target.y },
      ],
    })
  }

  return { nodes, edges }
}
