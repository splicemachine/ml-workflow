{{- define "kdeploy.identifier" -}}
{{- printf "model-%s" .Values.model.runId -}}
{{- end -}}

{{- define "kdeploy.dbEnvs" -}}
{{- with .Values.entity -}}
- name: DB_USER
  valueFrom:
    secretKeyRef:
      name: model-{{ .Values.model.runId }}-db-secret
      key: DB_USER
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: model-{{ .Values.model.runId }}-db-secret
      key: DB_PASSWORD
- name: DB_HOST
  valueFrom:
    secretKeyRef:
      name: model-{{ .Values.model.runId }}-db-secret
      key: DB_HOST
{{- end -}}
{{ -end -}}

{{- define "kdeploy.ownerRef" -}}
ownerReferences:
- apiVersion: v1
  controller: true
  kind: Pod
  name: {{ .Values.k8s.ownerPod }}
  uid: {{ .Values.k8s.ownerUID }}
{{- end -}}