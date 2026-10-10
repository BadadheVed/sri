'use client'

import { useMemo } from 'react'
import { ReactFlow, type Edge } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { useTopologyStream } from '@/hooks/useTopologyStream'
import {
  memoLayout,
  type TopologyEdge,
  type TopologyNode,
} from '@/lib/topology'
import TopologyNodeCard, {
  type ServiceFlowNode,
} from './TopologyNodeCard'

const nodeTypes = { service: TopologyNodeCard }

// Identity caches: unchanged snapshot objects map to the same React Flow objects.
const nodeCache = new WeakMap<TopologyNode, { x: number; y: number; flow: ServiceFlowNode }>()
const edgeCache = new WeakMap<TopologyEdge, Edge>()

function wsUrl(): string | undefined {
  const base = process.env.NEXT_PUBLIC_TOPOLOGY_WS_URL
  if (!base) return undefined
  const token = process.env.NEXT_PUBLIC_TOPOLOGY_TOKEN
  return token ? `${base}${base.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}` : base
}

function edgeLabel(e: TopologyEdge): string | undefined {
  const rps = e.requests_per_second
  const parts: string[] = []
  if (rps > 0) parts.push(`${rps < 10 ? rps.toFixed(1) : Math.round(rps)} req/s`)
  if (e.errors_per_second > 0) parts.push('err')
  return parts.length ? parts.join(' ') : undefined
}

export default function TopologyPage() {
  const url = wsUrl()
  const { namespaces, snapshot, status, selected, subscribe } = useTopologyStream(url)

  const { nodes, edges } = useMemo(() => {
    if (!snapshot) return { nodes: [] as ServiceFlowNode[], edges: [] as Edge[] }
    const positions = memoLayout(snapshot.nodes)
    const ids = new Set(snapshot.nodes.map((n) => n.id))

    const nodes = snapshot.nodes.map((n) => {
      const pos = positions[n.id]
      const cached = nodeCache.get(n)
      if (cached && cached.x === pos.x && cached.y === pos.y) return cached.flow
      const flow: ServiceFlowNode = {
        id: n.id,
        type: 'service',
        position: { x: pos.x, y: pos.y },
        data: { node: n },
      }
      nodeCache.set(n, { x: pos.x, y: pos.y, flow })
      return flow
    })

    const edges = snapshot.edges
      .filter((e) => ids.has(e.source) && ids.has(e.target))
      .map((e) => {
        const cached = edgeCache.get(e)
        if (cached) return cached
        const hasErr = e.errors_per_second > 0
        const flow: Edge = {
          id: `${e.source}>${e.target}`,
          source: e.source,
          target: e.target,
          animated: true,
          label: edgeLabel(e),
          style: hasErr ? { stroke: '#c00', strokeDasharray: '6 4' } : undefined,
        }
        edgeCache.set(e, flow)
        return flow
      })
    return { nodes, edges }
  }, [snapshot])

  const toggle = (name: string) => {
    const next = selected.includes(name)
      ? selected.filter((s) => s !== name)
      : [...selected, name]
    subscribe(next)
  }

  return (
    <div style={{ display: 'flex', height: '100vh' }}>
      <aside style={{ width: 240, padding: 12, overflowY: 'auto', borderRight: '1px solid #ccc' }}>
        <h2 style={{ marginTop: 0 }}>Namespaces</h2>
        <p data-testid="status">Status: {status}</p>
        {!url && <p>NEXT_PUBLIC_TOPOLOGY_WS_URL is not set.</p>}
        <ul style={{ listStyle: 'none', padding: 0 }}>
          {namespaces.map((ns) => (
            <li key={ns.name}>
              <label>
                <input
                  type="checkbox"
                  checked={selected.includes(ns.name)}
                  onChange={() => toggle(ns.name)}
                />{' '}
                {ns.name} ({ns.services})
              </label>
            </li>
          ))}
        </ul>
      </aside>
      <main style={{ flex: 1 }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onlyRenderVisibleElements
          fitView
          nodesConnectable={false}
        />
      </main>
    </div>
  )
}
