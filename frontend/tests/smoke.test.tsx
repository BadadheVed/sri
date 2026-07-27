import { render, screen } from '@testing-library/react'
import '@testing-library/jest-dom'
import Home from '../app/page'

test('renders the SRE Platform landing page', () => {
  render(<Home />)
  expect(screen.getByText(/SRE Platform/i)).toBeInTheDocument()
})
