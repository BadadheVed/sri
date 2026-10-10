'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import {
  reconcileSnapshot,
  type NamespaceInfo,
  type Snapshot,
} from '@/lib/topology'

export type StreamStatus = 'connecting' | 'open' | 'reconnecting' | 'closed'

const BASE_DELAY_MS = 1000
const MAX_DELAY_MS = 15000
const FLUSH_INTERVAL_MS = 1000

export function useTopologyStream(url: string | undefined) {
  const [namespaces, setNamespaces] = useState<NamespaceInfo[]>([])
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [status, setStatus] = useState<StreamStatus>(url ? 'connecting' : 'closed')
  const [selected, setSelected] = useState<string[]>([])

  const wsRef = useRef<WebSocket | null>(null)
  const selectedRef = useRef<string[]>([])

  const sendSubscribe = useCallback((names: string[]) => {
    const ws = wsRef.current
    if (ws && ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'subscribe', namespaces: names }))
    }
  }, [])

  const subscribe = useCallback(
    (names: string[]) => {
      selectedRef.current = names
      setSelected(names)
      sendSubscribe(names)
    },
    [sendSubscribe],
  )

  useEffect(() => {
    if (!url) return
    let disposed = false
    let attempt = 0
    let retryTimer: ReturnType<typeof setTimeout> | undefined
    let flushTimer: ReturnType<typeof setTimeout> | undefined
    let lastFlush = 0
    let pending: Snapshot | null = null

    const flush = () => {
      flushTimer = undefined
      if (!pending) return
      const next = pending
      pending = null
      lastFlush = Date.now()
      setSnapshot((prev) => reconcileSnapshot(prev, next))
    }

    const queueSnapshot = (s: Snapshot) => {
      pending = s
      if (flushTimer !== undefined) return
      const wait = Math.max(0, lastFlush + FLUSH_INTERVAL_MS - Date.now())
      if (wait === 0) flush()
      else flushTimer = setTimeout(flush, wait)
    }

    const connect = () => {
      const ws = new WebSocket(url)
      wsRef.current = ws

      ws.onopen = () => {
        if (disposed) return
        attempt = 0
        setStatus('open')
        if (selectedRef.current.length > 0) {
          ws.send(
            JSON.stringify({ type: 'subscribe', namespaces: selectedRef.current }),
          )
        }
      }

      ws.onmessage = (ev) => {
        if (disposed) return
        let msg: {
          type?: string
          items?: NamespaceInfo[]
          ts?: string
          nodes?: Snapshot['nodes']
          edges?: Snapshot['edges']
        }
        try {
          msg = JSON.parse(ev.data as string)
        } catch {
          return
        }
        if (msg.type === 'namespaces' && Array.isArray(msg.items)) {
          setNamespaces(msg.items)
          if (selectedRef.current.length === 0 && msg.items.length > 0) {
            subscribe([msg.items[0].name])
          }
        } else if (msg.type === 'snapshot') {
          queueSnapshot({
            ts: msg.ts ?? '',
            nodes: msg.nodes ?? [],
            edges: msg.edges ?? [],
          })
        }
      }

      ws.onerror = () => {
        // onclose follows and drives reconnect
      }

      ws.onclose = () => {
        if (disposed) return
        wsRef.current = null
        setStatus('reconnecting')
        const delay = Math.min(BASE_DELAY_MS * 2 ** attempt, MAX_DELAY_MS)
        attempt += 1
        retryTimer = setTimeout(connect, delay)
      }
    }

    connect()

    return () => {
      disposed = true
      if (retryTimer !== undefined) clearTimeout(retryTimer)
      if (flushTimer !== undefined) clearTimeout(flushTimer)
      const ws = wsRef.current
      wsRef.current = null
      if (ws) {
        ws.onclose = null
        ws.onmessage = null
        ws.onopen = null
        ws.onerror = null
        ws.close()
        // mock/real close both fine; handlers already detached
      }
    }
  }, [url, subscribe])

  return { namespaces, snapshot, status, selected, subscribe }
}
