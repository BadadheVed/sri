export interface TopologyPort {
  port: number
  protocol: string
  targetPort: string
}

export interface TopologyNode {
  id: string
  namespace: string
  name: string
  stub: boolean
  ports: TopologyPort[]
}

export interface TopologyEdge {
  source: string
  target: string
  requests_per_second: number
  errors_per_second: number
}

export interface Snapshot {
  type?: 'snapshot'
  ts: string
  nodes: TopologyNode[]
  edges: TopologyEdge[]
}

export interface NamespaceInfo {
  name: string
  services: number
}

export interface Position {
  x: number
  y: number
}

export const COL_WIDTH = 300
export const ROW_HEIGHT = 130

const byString = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0)

/** One column per namespace (sorted), nodes stacked by name. Deterministic. */
export function layout(nodes: TopologyNode[]): Record<string, Position> {
  const cols = new Map<string, TopologyNode[]>()
  for (const node of nodes) {
    const list = cols.get(node.namespace) ?? []
    list.push(node)
    cols.set(node.namespace, list)
  }
  const positions: Record<string, Position> = {}
  Array.from(cols.keys())
    .sort(byString)
    .forEach((ns, col) => {
      cols
        .get(ns)!
        .sort((a, b) => byString(a.name, b.name) || byString(a.id, b.id))
        .forEach((node, row) => {
          positions[node.id] = { x: col * COL_WIDTH, y: row * ROW_HEIGHT }
        })
    })
  return positions
}

export interface LayoutCache {
  key: string
  positions: Record<string, Position>
}

/** Recompute layout only when the set of node ids changes. */
export function layoutIfNeeded(
  prev: LayoutCache | null,
  nodes: TopologyNode[],
): LayoutCache {
  const key = nodes
    .map((n) => n.id)
    .sort(byString)
    .join('\n')
  if (prev && prev.key === key) return prev
  return { key, positions: layout(nodes) }
}

function reuse<T>(prev: T[], next: T[], keyOf: (t: T) => string): T[] {
  const old = new Map(prev.map((p) => [keyOf(p), p]))
  return next.map((item) => {
    const p = old.get(keyOf(item))
    return p !== undefined && JSON.stringify(p) === JSON.stringify(item) ? p : item
  })
}

/**
 * Returns `next` with unchanged nodes/edges replaced by their `prev`
 * instances. Returns `prev` itself when nothing changed.
 */
export function reconcileSnapshot(prev: Snapshot | null, next: Snapshot): Snapshot {
  if (!prev) return next
  const nodes = reuse(prev.nodes, next.nodes, (n) => n.id)
  const edges = reuse(prev.edges, next.edges, (e) => `${e.source}>${e.target}`)
  const same =
    nodes.length === prev.nodes.length &&
    edges.length === prev.edges.length &&
    nodes.every((n, i) => n === prev.nodes[i]) &&
    edges.every((e, i) => e === prev.edges[i])
  if (same) return prev
  return { ...next, nodes, edges }
}

let lastLayout: LayoutCache | null = null

/** Module-memoized layout: positions reused while the node id set is unchanged. */
export function memoLayout(nodes: TopologyNode[]): Record<string, Position> {
  lastLayout = layoutIfNeeded(lastLayout, nodes)
  return lastLayout.positions
}
