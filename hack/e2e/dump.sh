#!/usr/bin/env bash
set -u

kubectl get pods,svc,roleclaims,databaseclaims -A || true
kubectl get roleclaims,databaseclaims -A -o yaml || true
kubectl -n cnpg-dbclaim-system logs -l app.kubernetes.io/name=dbclaim-operator --tail=500 || true
kubectl -n cnpg-system describe clusters.postgresql.cnpg.io/shared-pg || true
kubectl get events -A --sort-by=.lastTimestamp | tail -100 || true
