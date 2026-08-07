{{/*
Base name of the chart.
*/}}
{{- define "sage.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name, used as the prefix for every namespaced resource.
*/}}
{{- define "sage.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "sage.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sage.labels" -}}
helm.sh/chart: {{ include "sage.chart" . }}
{{ include "sage.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "sage.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sage.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Per-component names. Namespaced resources (Deployment, Service, ServiceAccount,
ConfigMap, Secret) only need to be unique within the release's namespace, so
release-name-based uniqueness is enough.
*/}}
{{- define "sage.backend.fullname" -}}
{{- printf "%s-backend" (include "sage.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sage.mcpExecute.fullname" -}}
{{- printf "%s-mcp-execute-server" (include "sage.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sage.backend.selectorLabels" -}}
{{ include "sage.selectorLabels" . }}
app.kubernetes.io/component: backend
{{- end -}}

{{- define "sage.mcpExecute.selectorLabels" -}}
{{ include "sage.selectorLabels" . }}
app.kubernetes.io/component: mcp-execute-server
{{- end -}}

{{/*
ClusterRole/ClusterRoleBinding names are cluster-scoped, so release-name
uniqueness alone isn't enough — two installs of this chart into different
namespaces would otherwise collide on the same ClusterRole name. Fold the
namespace in too.
*/}}
{{- define "sage.rbac.backend.name" -}}
{{- printf "%s-%s-backend" (include "sage.fullname" .) .Release.Namespace | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sage.rbac.mcpExecute.name" -}}
{{- printf "%s-%s-mcp-execute-server" (include "sage.fullname" .) .Release.Namespace | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Name of the Secret holding DATABASE_URL / SLACK_BOT_TOKEN / SLACK_SIGNING_SECRET
/ MCP_EXECUTE_TOKEN — either the chart-managed one, or a pre-existing Secret
the caller points at via secrets.existingSecret. Every template that needs
this Secret's name should call this helper rather than hardcoding either form,
so the two paths (chart-managed vs. bring-your-own) stay a single call site.
*/}}
{{- define "sage.secretName" -}}
{{- .Values.secrets.existingSecret | default (printf "%s-secrets" (include "sage.fullname" .)) -}}
{{- end -}}

{{/*
In-cluster DNS name of the bundled Postgres, when postgresql.enabled=true.
Fixed rather than derived from the Bitnami subchart's own fullname logic —
simpler, at the cost of two SAGE installs in the same namespace colliding on
this Service name (acceptable: SAGE is a platform-level singleton per
namespace, not a multi-tenant app).
*/}}
{{- define "sage.postgresqlHost" -}}
{{- .Values.postgresql.fullnameOverride -}}
{{- end -}}

{{/*
DATABASE_URL as a literal connection string. This helper is only used by
templates/secret.yaml when the chart manages the Secret (i.e. when
secrets.existingSecret is unset).
*/}}
{{- define "sage.databaseUrl" -}}
{{- if .Values.postgresql.enabled -}}
postgres://{{ .Values.postgresql.auth.username }}:{{ .Values.postgresql.auth.password | urlquery }}@{{ include "sage.postgresqlHost" . }}:5432/{{ .Values.postgresql.auth.database }}?sslmode=disable
{{- else -}}
{{- required "externalDatabase.url is required when postgresql.enabled=false and neither externalDatabase.existingSecret nor secrets.existingSecret is set" .Values.externalDatabase.url -}}
{{- end -}}
{{- end -}}

{{/*
Validates remediation.mode at render time instead of letting an invalid
value reach the running app (where settings.Load() would only catch it as a
log.Fatal crash-loop after the pod already started). Mirrors the same
fail-closed rule gate.Evaluate itself enforces in Go.
*/}}
{{- define "sage.validatedMode" -}}
{{- if or (eq .Values.remediation.mode "auto") (eq .Values.remediation.mode "manual") -}}
{{- .Values.remediation.mode -}}
{{- else -}}
{{- fail (printf "remediation.mode must be exactly \"auto\" or \"manual\", got %q" .Values.remediation.mode) -}}
{{- end -}}
{{- end -}}

{{/*
In-cluster URL of the MCP execute server — the value backend/ uses to reach
it. ClusterIP-only, never exposed externally (see templates/service-mcp-execute.yaml).
*/}}
{{- define "sage.mcpExecuteUrl" -}}
http://{{ include "sage.mcpExecute.fullname" . }}:{{ .Values.mcpExecute.port }}
{{- end -}}
