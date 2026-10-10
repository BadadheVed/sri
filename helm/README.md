# SAGE Helm Chart

## Langfuse (prompt management) prerequisite

`langfuse.enabled` (default `false`) makes `ai/` fetch its investigation
prompt from [Langfuse](https://langfuse.com) instead of using a local
fallback — see `ai/ai/prompts.py`. Not installed by this chart (real
infrastructure — Langfuse's self-hosted stack bundles its own
Postgres+ClickHouse+Redis, and its Helm chart v2.0.0+ requires a
pre-installed ClickHouse Kubernetes Operator and cert-manager, a real
cluster-scoped prerequisite):

1. Deploy self-hosted Langfuse (re-verify against
   [langfuse/langfuse-k8s](https://github.com/langfuse/langfuse-k8s) at
   execution time — chart major versions change prerequisites):
   ```bash
   helm repo add langfuse https://langfuse.github.io/langfuse-k8s
   helm repo update
   helm install langfuse langfuse/langfuse -n langfuse --create-namespace
   ```
2. Once Langfuse is reachable, seed the investigation prompt:
   ```bash
   LANGFUSE_ENABLED=true LANGFUSE_HOST=<url> \
   LANGFUSE_PUBLIC_KEY=<key> LANGFUSE_SECRET_KEY=<secret> \
   python -m ai.seed_prompt
   ```
3. Deploy SAGE with Langfuse enabled:
   ```bash
   helm upgrade sage ./helm -n sage -f helm/values.secret.yaml \
     --set langfuse.enabled=true \
     --set langfuse.host=<your-langfuse-url> \
     --set langfusePublicKey=<your-public-key> \
     --set langfuseSecretKey=<your-secret-key>
   ```

## Beyla (live cluster topology)

Optional, off by default (`beyla.enabled=false`). When enabled the chart
deploys Grafana Beyla as a DaemonSet (label `app.kubernetes.io/name=beyla`)
and sets `BEYLA_ENABLED`, `BEYLA_POD_SELECTOR`, `BEYLA_PORT`,
`BEYLA_NAMESPACE` and `METRICS_POLL_INTERVAL_SECONDS` on the backend.

- Beyla is eBPF-based: the DaemonSet runs `privileged` with `hostPID: true`
  and a read-only ClusterRole (pods, services, nodes, namespaces,
  replicasets). Only enable it on clusters where you accept that.
- `beyla.namespaces` limits instrumentation (empty = all namespaces). Entries
  are Beyla 2.x glob patterns (e.g. `shop`, `team-*`), each rendered as its own
  `discovery.instrument` entry; empty renders `k8s_namespace: "*"`.
- Only the `application` Prometheus feature is exported (what the backend
  reads): HTTP server histograms for `/ws/metrics`, and
  `http_client_request_duration_seconds` counts, from which the topology
  edges are derived (Beyla 3.x does not emit `traces_service_graph_*` here).
- The live topology stream `/ws/topology` needs its own token: set
  `topologyWsToken` (or `TOPOLOGY_WS_TOKEN` in `secrets.existingSecret`).
  It has no default; when empty the endpoint is disabled (404) and the backend
  logs that at startup. It is sent by browsers as `?token=`, so never reuse
  `mcpReadonlyToken`.
- EKS Auto Mode caveat: nodes are AWS-managed with a locked-down OS, and
  privileged/hostPID workloads may be rejected or unable to load eBPF
  programs. Verify on a test cluster first; managed-node-group EKS works.

```bash
helm upgrade sage ./helm -n sage -f helm/values.secret.yaml --set beyla.enabled=true \
  --set topologyWsToken=<random-token>
```
