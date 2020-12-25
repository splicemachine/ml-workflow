{{- define "retraining.identifier" -}}
{{- printf "rt-%s-%s" .Values.entity.entityId .Values.entity.name -}}
{{- end -}}

{{- define "retraining.dbEnvs" -}}
{{- with .Values.entity -}}
- name: DB_USER
  valueFrom:
    secretKeyRef:
      name: rt-{{ .entityId }}-{{ .name }}-db-secret
      key: DB_USER
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: rt-{{ .entityId }}-{{ .name }}-db-secret
      key: DB_PASSWORD
- name: DB_HOST
  valueFrom:
    secretKeyRef:
      name: rt-{{ .entityId }}-{{ .name }}-db-secret
      key: DB_HOST
{{- end -}}
{{ -end -}}

{{- define "retraining.ownerRef" }}
ownerReferences:
- apiVersion: v1
  controller: true
  kind: Pod
  name: {{ .Values.k8s.ownerPod }}
  uid: {{ .Values.k8s.ownerUID }}
{{- end }}