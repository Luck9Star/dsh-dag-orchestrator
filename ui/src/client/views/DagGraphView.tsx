/**
 * DAG graph: header chrome plus an SVG layout of spec tasks.
 * @module dsh-dag-view/client/views/DagGraphView
 */

import { computeLayout, type Layout, type LayoutNode } from '../../core/graph.ts'
import { formatCounts, relativeTime, stateTone } from '../../core/format.ts'
import type { RunSummary, SpecView, TaskRow, TaskState } from '../../core/types.ts'
import { getDict, type DagViewKey, type Locale } from '../locales.ts'
import styles from '../styles.module.css'

/**
 * Flattened run snapshot the graph renders: summary fields plus the spec
 * (and optional task rows for per-node state).
 */
export interface RunView extends RunSummary {
  readonly spec: SpecView
  readonly tasks?: readonly TaskRow[]
}

export interface DagGraphViewProps {
  readonly run: RunView
  readonly locale: Locale
  readonly selectedTaskId?: string
  readonly onSelectTask: (taskId: string) => void
  readonly onRefresh: () => void
  readonly lastUpdatedAt?: number
  readonly loading?: boolean
}

const LEGEND_STATES = [
  'pending',
  'ready',
  'running',
  'succeeded',
  'failed',
  'blocked',
  'cancelled',
  'retry_wait',
] as const

const VIEW_PAD = 24

function tx(locale: Locale, key: DagViewKey, params?: Record<string, string>): string {
  let text: string = getDict(locale)[key]
  if (params !== undefined) {
    for (const [name, value] of Object.entries(params)) {
      text = text.replaceAll(`{${name}}`, value)
    }
  }
  return text
}

function stateLabel(locale: Locale, state: string): string {
  const key = `state.${state}` as DagViewKey
  const dict = getDict(locale)
  return key in dict ? dict[key] : state
}

function toneClass(state: string): string {
  const key = stateTone(state)
  return styles[key] ?? key
}

function shortId(id: string): string {
  return id.slice(0, 8)
}

function truncateMiddle(id: string): string {
  if (id.length <= 18) return id
  return `${id.slice(0, 8)}…${id.slice(-7)}`
}

function hexagonPath(x: number, y: number, w: number, h: number): string {
  const inset = Math.min(w * 0.16, 20)
  const midY = y + h / 2
  return [
    `M ${x + inset} ${y}`,
    `L ${x + w - inset} ${y}`,
    `L ${x + w} ${midY}`,
    `L ${x + w - inset} ${y + h}`,
    `L ${x + inset} ${y + h}`,
    `L ${x} ${midY}`,
    'Z',
  ].join(' ')
}

function viewBoxOf(layout: Layout): string {
  let minX = 0
  let minY = 0
  let maxX = 320
  let maxY = 180
  const points: Array<{ x: number; y: number }> = []
  for (const node of layout.nodes) {
    points.push({ x: node.x, y: node.y }, { x: node.x + node.w, y: node.y + node.h })
  }
  for (const edge of layout.edges) {
    for (const point of edge.points) points.push(point)
  }
  if (points.length > 0) {
    minX = Math.min(...points.map((p) => p.x))
    minY = Math.min(...points.map((p) => p.y))
    maxX = Math.max(...points.map((p) => p.x))
    maxY = Math.max(...points.map((p) => p.y))
  }
  const width = Math.max(maxX - minX, 1) + VIEW_PAD * 2
  const height = Math.max(maxY - minY, 1) + VIEW_PAD * 2
  return `${minX - VIEW_PAD} ${minY - VIEW_PAD} ${width} ${height}`
}

function NodeShape({
  node,
  state,
}: {
  node: LayoutNode
  state: TaskState | string
}): JSX.Element {
  const tone = toneClass(state)
  const pulse = state === 'running' ? styles.pulse : ''
  const shared = `${styles.node} ${tone} ${pulse}`.trim()
  if (node.kind === 'approval') {
    return (
      <path
        className={`${shared} ${styles.nodeApproval}`}
        d={hexagonPath(node.x, node.y, node.w, node.h)}
      />
    )
  }
  if (node.kind === 'merge') {
    return (
      <>
        <rect
          className={`${shared} ${styles.nodeMerge}`}
          x={node.x}
          y={node.y}
          width={node.w}
          height={node.h}
        />
        <rect
          className={`${styles.node} ${styles.nodeMerge} ${tone}`}
          x={node.x + 5}
          y={node.y + 5}
          width={Math.max(node.w - 10, 1)}
          height={Math.max(node.h - 10, 1)}
        />
      </>
    )
  }
  return (
    <rect
      className={`${shared} ${styles.nodeAgent}`}
      x={node.x}
      y={node.y}
      width={node.w}
      height={node.h}
      rx={8}
    />
  )
}

/** Header + SVG graph for one run aggregate. */
export function DagGraphView({
  run,
  locale,
  selectedTaskId,
  onSelectTask,
  onRefresh,
  lastUpdatedAt,
  loading,
}: DagGraphViewProps): JSX.Element {
  const layout = computeLayout(run.spec)
  const stateById = new Map((run.tasks ?? []).map((row) => [row.id, row.state]))

  return (
    <div className={styles.centerPanel}>
      <div className={styles.appHeader}>
        <span className={styles.runName}>{run.name}</span>
        <span className={styles.runIdShort}>{shortId(run.run_id)}</span>
        <span className={`${styles.stateBadge} ${toneClass(run.state)}`}>
          {stateLabel(locale, run.state)}
        </span>
        <span className={styles.countsSummary}>{formatCounts(run.counts, locale)}</span>
        <div className={styles.refreshRow}>
          <button type="button" className={styles.backButton} onClick={onRefresh}>
            {tx(locale, 'action.refresh')}
          </button>
          {lastUpdatedAt !== undefined ? (
            <span className={styles.refreshTime}>
              {tx(locale, 'refresh.updated', {
                t: relativeTime(Date.now(), lastUpdatedAt, locale),
              })}
            </span>
          ) : null}
        </div>
      </div>
      {loading === true ? (
        <div className={styles.loadingState}>{tx(locale, 'loading.graph')}</div>
      ) : null}
      {run.spec.tasks.length === 0 ? (
        <div className={styles.emptyState}>{tx(locale, 'empty.graph')}</div>
      ) : (
        <div className={styles.graphArea}>
          <svg
            className={styles.graphSvg}
            viewBox={viewBoxOf(layout)}
            width="100%"
            height="100%"
          >
            {layout.edges.map((edge) => (
              <polyline
                key={`${edge.from}->${edge.to}`}
                className={styles.edgeLine}
                points={edge.points.map((p) => `${p.x},${p.y}`).join(' ')}
              />
            ))}
            {layout.nodes.map((node) => {
              const taskState = stateById.get(node.id) ?? 'pending'
              const selected = selectedTaskId === node.id
              return (
                <g
                  key={node.id}
                  data-node-id={node.id}
                  className={selected ? styles.tabActive : undefined}
                  onClick={() => { onSelectTask(node.id) }}
                >
                  <NodeShape node={node} state={taskState} />
                  <text
                    className={styles.nodeLabel}
                    x={node.x + node.w / 2}
                    y={node.y + node.h / 2}
                    textAnchor="middle"
                    dominantBaseline="middle"
                  >
                    {truncateMiddle(node.id)}
                  </text>
                </g>
              )
            })}
          </svg>
        </div>
      )}
      <div className={styles.legend}>
        {LEGEND_STATES.map((state) => (
          <span key={state} className={styles.legendItem}>
            <span className={`${styles.legendSwatch} ${toneClass(state)}`} />
            <span>{stateLabel(locale, state)}</span>
          </span>
        ))}
      </div>
    </div>
  )
}
