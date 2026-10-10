import Link from 'next/link'

export default function Home() {
  return (
    <main>
      <h1>SRE Platform</h1>
      <p>Dashboard — incidents, history, and audit log land here in a later plan.</p>
      <p>
        <Link href="/topology">Service topology</Link>
      </p>
    </main>
  )
}
