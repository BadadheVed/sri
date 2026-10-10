import {
  layout,
  reconcileSnapshot,
  layoutIfNeeded,
  COL_WIDTH,
  ROW_HEIGHT,
  type TopologyNode,
  type Snapshot,
} from '@/lib/topology'

const n = (ns: string, name: string, stub = false): TopologyNode => ({
  id: `${ns}/${name}`,
  namespace: ns,
  name,
  stub,
  ports: [{ port: 80, protocol: 'TCP', targetPort: '8080' }],
})

const snap = (nodes: TopologyNode[], edges: Snapshot['edges'] = []): Snapshot => ({
  type: 'snapshot',
  ts: '2026-01-01T00:00:00Z',
  nodes,
  edges,
})

describe('layout', () => {
  it('one column per namespace, stacked vertically, sorted', () => {
    const pos = layout([n('b', 'z'), n('a', 'y'), n('a', 'x'), n('b', 'w', true)])
    expect(pos['a/x']).toEqual({ x: 0, y: 0 })
    expect(pos['a/y']).toEqual({ x: 0, y: ROW_HEIGHT })
    expect(pos['b/w']).toEqual({ x: COL_WIDTH, y: 0 })
    expect(pos['b/z']).toEqual({ x: COL_WIDTH, y: ROW_HEIGHT })
  })

  it('is deterministic regardless of input order', () => {
    const a = [n('a', 'x'), n('b', 'y'), n('a', 'z')]
    expect(layout(a)).toEqual(layout([...a].reverse()))
  })
})

describe('layoutIfNeeded', () => {
  it('reuses cache when id set unchanged', () => {
    const c1 = layoutIfNeeded(null, [n('a', 'x'), n('a', 'y')])
    const c2 = layoutIfNeeded(c1, [n('a', 'y'), n('a', 'x')])
    expect(c2).toBe(c1)
  })
  it('recomputes when ids change', () => {
    const c1 = layoutIfNeeded(null, [n('a', 'x')])
    const c2 = layoutIfNeeded(c1, [n('a', 'x'), n('a', 'y')])
    expect(c2).not.toBe(c1)
    expect(c2.positions['a/y']).toBeDefined()
  })
})

describe('reconcileSnapshot', () => {
  it('returns prev when nothing changed', () => {
    const prev = snap([n('a', 'x')], [{ source: 'a/x', target: 'a/y', requests_per_second: 1, errors_per_second: 0 }])
    const next = JSON.parse(JSON.stringify(prev))
    expect(reconcileSnapshot(prev, next)).toBe(prev)
  })
  it('keeps identity of unchanged nodes/edges only', () => {
    const e1 = { source: 'a/x', target: 'a/y', requests_per_second: 1, errors_per_second: 0 }
    const e2 = { source: 'a/y', target: 'a/x', requests_per_second: 2, errors_per_second: 0 }
    const prev = snap([n('a', 'x'), n('a', 'y')], [e1, e2])
    const next = snap(
      [n('a', 'x'), { ...n('a', 'y'), stub: true }],
      [{ ...e1 }, { ...e2, requests_per_second: 5 }],
    )
    const out = reconcileSnapshot(prev, next)
    expect(out).not.toBe(prev)
    expect(out.nodes[0]).toBe(prev.nodes[0])
    expect(out.nodes[1]).not.toBe(prev.nodes[1])
    expect(out.edges[0]).toBe(prev.edges[0])
    expect(out.edges[1].requests_per_second).toBe(5)
  })
  it('handles null prev', () => {
    const next = snap([n('a', 'x')])
    expect(reconcileSnapshot(null, next)).toBe(next)
  })
})
