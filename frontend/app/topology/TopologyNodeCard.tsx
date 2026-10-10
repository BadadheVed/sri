'use client'

import { Handle, Position, type Node, type NodeProps } from '@xyflow/react'
import type { TopologyNode } from '@/lib/topology'

export type ServiceNodeData = { node: TopologyNode } & Record<string, unknown>
export type ServiceFlowNode = Node<ServiceNodeData, 'service'>

export default function TopologyNodeCard({ data }: NodeProps<ServiceFlowNode>) {
  const { node } = data
  return (
    <div
      style={{
        border: '1px solid #888',
        borderRadius: 4,
        padding: 8,
        width: 220,
        background: '#fff',
        color: '#000',
        opacity: node.stub ? 0.45 : 1,
        borderStyle: node.stub ? 'dashed' : 'solid',
        fontSize: 12,
      }}
    >
      <Handle type="target" position={Position.Left} />
      <strong>{node.name}</strong>
      {node.stub && <em> (stub, {node.namespace})</em>}
      <ul style={{ margin: '4px 0 0', paddingLeft: 16 }}>
        {node.ports.map((p) => (
          <li key={`${p.port}/${p.protocol}`}>
            {p.port}/{p.protocol}→{p.targetPort}
          </li>
        ))}
      </ul>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
