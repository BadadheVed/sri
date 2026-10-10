import { renderHook, act } from '@testing-library/react'
import { useTopologyStream } from '@/hooks/useTopologyStream'

class MockWS {
  static instances: MockWS[] = []
  onopen: (() => void) | null = null
  onclose: (() => void) | null = null
  onmessage: ((e: { data: string }) => void) | null = null
  onerror: (() => void) | null = null
  sent: string[] = []
  closed = false
  readyState = 0
  constructor(public url: string) {
    MockWS.instances.push(this)
  }
  send(d: string) {
    this.sent.push(d)
  }
  close() {
    this.closed = true
    this.readyState = 3
    this.onclose?.()
  }
  open() {
    this.readyState = 1
    this.onopen?.()
  }
  recv(o: unknown) {
    this.onmessage?.({ data: JSON.stringify(o) })
  }
  drop() {
    this.readyState = 3
    this.onclose?.()
  }
}

const last = () => MockWS.instances[MockWS.instances.length - 1]
const snap = (n: string) => ({
  type: 'snapshot',
  ts: 't',
  nodes: [{ id: `a/${n}`, namespace: 'a', name: n, stub: false, ports: [] }],
  edges: [],
})
const nsMsg = { type: 'namespaces', items: [{ name: 'a', services: 2 }, { name: 'b', services: 1 }] }

beforeEach(() => {
  jest.useFakeTimers()
  MockWS.instances = []
  ;(globalThis as unknown as { WebSocket: unknown }).WebSocket = MockWS
})
afterEach(() => jest.useRealTimers())

const setup = () => renderHook(() => useTopologyStream('ws://x/ws'))

it('connects and reports status', () => {
  const { result } = setup()
  expect(result.current.status).toBe('connecting')
  act(() => last().open())
  expect(result.current.status).toBe('open')
})

it('auto-subscribes to first namespace', () => {
  const { result } = setup()
  act(() => last().open())
  act(() => last().recv(nsMsg))
  expect(result.current.namespaces).toHaveLength(2)
  expect(result.current.selected).toEqual(['a'])
  expect(last().sent.map((s) => JSON.parse(s))).toEqual([{ type: 'subscribe', namespaces: ['a'] }])
})

it('auto-subscribes on a later namespaces frame when the first one was empty', () => {
  const { result } = setup()
  act(() => last().open())
  act(() => last().recv({ type: 'namespaces', items: [] }))
  expect(result.current.selected).toEqual([])
  expect(last().sent).toEqual([])
  act(() => last().recv(nsMsg))
  expect(result.current.namespaces).toHaveLength(2)
  expect(result.current.selected).toEqual(['a'])
  expect(last().sent.map((s) => JSON.parse(s))).toEqual([{ type: 'subscribe', namespaces: ['a'] }])
  // a further refresh does not override the selection
  act(() => last().recv({ type: 'namespaces', items: [{ name: 'b', services: 1 }, { name: 'a', services: 3 }] }))
  expect(result.current.selected).toEqual(['a'])
  expect(result.current.namespaces[1].services).toBe(3)
  expect(last().sent).toHaveLength(1)
})

it('does not auto-subscribe if caller already chose', () => {
  const { result } = setup()
  act(() => last().open())
  act(() => result.current.subscribe(['b']))
  act(() => last().recv(nsMsg))
  expect(result.current.selected).toEqual(['b'])
  expect(last().sent.map((s) => JSON.parse(s))).toEqual([{ type: 'subscribe', namespaces: ['b'] }])
})

it('reconnects with exponential backoff capped at 15s and resubscribes', () => {
  const { result } = setup()
  act(() => last().open())
  act(() => last().recv(nsMsg))
  const delays: number[] = []
  for (let i = 0; i < 6; i++) {
    const count = MockWS.instances.length
    act(() => last().drop())
    expect(result.current.status).toBe('reconnecting')
    let waited = 0
    while (MockWS.instances.length === count) {
      act(() => jest.advanceTimersByTime(500))
      waited += 500
    }
    delays.push(waited)
  }
  expect(delays).toEqual([1000, 2000, 4000, 8000, 15000, 15000])
  act(() => last().open())
  expect(last().sent.map((s) => JSON.parse(s))).toEqual([{ type: 'subscribe', namespaces: ['a'] }])
  // backoff resets after a successful open
  const c = MockWS.instances.length
  act(() => last().drop())
  act(() => jest.advanceTimersByTime(1000))
  expect(MockWS.instances.length).toBe(c + 1)
})

it('batches snapshots to at most ~1/s keeping the latest', () => {
  const { result } = setup()
  act(() => last().open())
  act(() => last().recv(snap('s1')))
  expect(result.current.snapshot?.nodes[0].name).toBe('s1')
  act(() => last().recv(snap('s2')))
  act(() => last().recv(snap('s3')))
  expect(result.current.snapshot?.nodes[0].name).toBe('s1')
  act(() => jest.advanceTimersByTime(1000))
  expect(result.current.snapshot?.nodes[0].name).toBe('s3')
})

it('cleans up on unmount', () => {
  const { unmount } = setup()
  const ws = last()
  act(() => ws.open())
  unmount()
  expect(ws.closed).toBe(true)
  const c = MockWS.instances.length
  act(() => jest.advanceTimersByTime(60000))
  expect(MockWS.instances.length).toBe(c)
})
