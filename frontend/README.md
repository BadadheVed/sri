This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.

## Topology prototype

`/topology` shows a live service graph (React Flow) streamed from the backend websocket.
Copy `.env.local.example` to `.env.local` and set `NEXT_PUBLIC_TOPOLOGY_WS_URL` and
`NEXT_PUBLIC_TOPOLOGY_TOKEN` (dev-only; the token is exposed to the browser). The token must
equal the backend's `TOPOLOGY_WS_TOKEN` (helm value `topologyWsToken`), a dedicated token that is
deliberately not `MCP_READONLY_TOKEN`; with it unset the backend disables `/ws/topology` (404).
The page lists all namespaces, auto-selects the first, and re-subscribes as you toggle checkboxes.
The namespace list is refreshed live when namespaces or their service counts change.
Edges come from Beyla's client-side HTTP request counts: `errors_per_second` is the rate of 5xx
responses seen by the caller (each call counted once, so it never exceeds `requests_per_second`).
Server addresses that do not resolve to a cluster Service or pod show up as `external/<host>`
stub nodes.
