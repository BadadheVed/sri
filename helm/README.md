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
