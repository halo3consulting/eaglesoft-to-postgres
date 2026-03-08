{{/*
Expand the name of the chart.
*/}}
{{- define "eaglesoft-sync.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars (Kubernetes DNS name limit).
*/}}
{{- define "eaglesoft-sync.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "eaglesoft-sync.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "eaglesoft-sync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (subset of common labels, used for matching).
*/}}
{{- define "eaglesoft-sync.selectorLabels" -}}
app.kubernetes.io/name: {{ include "eaglesoft-sync.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Name of the Secret that holds credentials.
*/}}
{{- define "eaglesoft-sync.secretName" -}}
{{- if .Values.secrets.create }}
{{- include "eaglesoft-sync.fullname" . }}
{{- else }}
{{- required "secrets.existingSecretName is required when secrets.create is false" .Values.secrets.existingSecretName }}
{{- end }}
{{- end }}

{{/*
Name of the ConfigMap that holds sync_config.yaml.
*/}}
{{- define "eaglesoft-sync.configMapName" -}}
{{- printf "%s-config" (include "eaglesoft-sync.fullname" .) }}
{{- end }}
