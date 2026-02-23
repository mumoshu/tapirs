{{/*
TAPIRCluster resource name — uses .Values.name if set, otherwise the release name.
*/}}
{{- define "tapirs-cluster.name" -}}
{{- default .Release.Name .Values.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "tapirs-cluster.labels" -}}
app.kubernetes.io/name: tapirs
app.kubernetes.io/instance: {{ include "tapirs-cluster.name" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}
