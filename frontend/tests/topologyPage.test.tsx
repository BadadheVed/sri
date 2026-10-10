import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'

jest.mock('@xyflow/react', () => ({
  ReactFlow: ({ nodes, edges }: { nodes: unknown[]; edges: unknown[] }) => (
    <div data-testid="flow">{nodes.length}n/{edges.length}e</div>
  ),
  Handle: () => null,
  Position: { Left: 'left', Right: 'right' },
}))
jest.mock('@xyflow/react/dist/style.css', () => ({}), { virtual: true })
jest.mock('@/hooks/useTopologyStream', () => ({
  useTopologyStream: () => ({
    namespaces: [{ name: 'a', services: 2 }, { name: 'b', services: 1 }],
    snapshot: {
      ts: 't',
      nodes: [{ id: 'a/x', namespace: 'a', name: 'x', stub: false, ports: [] }],
      edges: [],
    },
    status: 'open',
    selected: ['a'],
    subscribe: jest.fn(),
  }),
}))

import TopologyPage from '../app/topology/page'

test('lists namespaces with counts and checks the selected one', () => {
  render(<TopologyPage />)
  expect(screen.getByLabelText(/a \(2\)/)).toBeChecked()
  expect(screen.getByLabelText(/b \(1\)/)).not.toBeChecked()
  expect(screen.getByTestId('status')).toHaveTextContent('open')
  expect(screen.getByTestId('flow')).toHaveTextContent('1n/0e')
})
