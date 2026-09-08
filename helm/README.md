# SAGE Helm Chart

## Pixie (eBPF observability) prerequisite

`pxMetrics.enabled` (default `false`) turns on two extra read-only MCP
tools — `get_pod_resource_usage`, `get_pod_traffic_stats` — backed by
[Pixie](https://px.dev), consumed by ai/'s LLM investigation loop for
CPU/traffic signal during incident diagnosis.

**Pixie is not installed by this chart.** Its Vizier install uses OLM
(Operator Lifecycle Manager, a cluster-singleton concern) plus a privileged,
host-level, eBPF-loading DaemonSet — tying its install/upgrade/removal to
`helm install sage`/`helm uninstall sage` risks colliding with another OLM
consumer on the cluster, or tearing down cluster-shared infrastructure when
someone just wants to remove SAGE. It's a real, separate infrastructure
prerequisite you stand up first:

1. Deploy self-hosted Pixie Cloud + Vizier — outline (re-verify each step
   against the live [pixie-io/pixie](https://github.com/pixie-io/pixie) repo
   at execution time; docs and release tags drift):
   - `git clone https://github.com/pixie-io/pixie.git`, check out the latest
     `release/cloud/vX.Y.Z` tag.
   - Deploy Pixie Cloud's dependencies (an Elastic Operator + Elasticsearch)
     into a `plc` namespace via `kustomize build k8s/cloud_deps/... | kubectl apply -f -`.
   - Deploy Pixie Cloud itself (`kustomize build k8s/cloud/public/ | kubectl apply -f -`)
     — Kratos/Hydra auth, API services, UI backend.
   - TLS via `mkcert`, DNS via the repo's `dev_dns_updater` (or your own DNS
     pointed at the cloud-proxy Service's external IP).
   - Generate a deploy key via the Cloud UI, then `px deploy --dev_cloud_namespace plc`
     to install Vizier into the target cluster.
   - Output needed for step 2 below: the reachable cloud/Vizier address and
     an API key.
2. Once Vizier is reachable, deploy SAGE with Pixie enabled:
   ```bash
   helm upgrade sage ./helm -n sage -f helm/values.secret.yaml \
     --set pxMetrics.enabled=true \
     --set pxMetrics.connMode=cloud \
     --set pxMetrics.vizierAddr=<your-cloud-addr>:443 \
     --set pxMetrics.clusterId=<your-cluster-id> \
     --set pixieApiKey=<your-pixie-api-key>
   ```
   If the self-hosted Pixie Cloud from step 1 uses a self-signed CA (e.g.
   the `mkcert`-based setup described above), also add
   `--set pxMetrics.insecureSkipTLSVerify=true`, which skips TLS
   certificate verification for the Pixie connection specifically (not the
   whole cluster). Caveat: due to a limitation in the `px.dev/pxapi` v0.5.0
   Go client this repo depends on, that flag only actually takes effect
   when `pxMetrics.vizierAddr` is a `*.cluster.local`-style in-cluster
   address — for a real external-looking domain (what the `mkcert` + DNS
   setup in step 1 typically produces), it is currently a no-op and TLS
   verification stays on, so the deployed process will still need to trust
   that CA some other way (e.g. via its container image's trust store)
   until pxapi exposes a general-purpose escape hatch.

## Langfuse (prompt management) prerequisite

`langfuse.enabled` (default `false`) makes `ai/` fetch its investigation
prompt from [Langfuse](https://langfuse.com) instead of using a local
fallback — see `ai/ai/prompts.py`. Not installed by this chart (real
infrastructure — Langfuse's self-hosted stack bundles its own
Postgres+ClickHouse+Redis, and its Helm chart v2.0.0+ requires a
pre-installed ClickHouse Kubernetes Operator and cert-manager, same
category of cluster-scoped prerequisite as Pixie's OLM requirement):

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
