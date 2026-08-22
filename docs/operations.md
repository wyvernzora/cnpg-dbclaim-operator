# Operations

## Argo CD health checks

Argo CD's `Synced` status only means the live Kubernetes object matches Git;
it does not know whether this operator reconciled external Postgres state. See
[config/samples/argocd-health-customizations.yaml](../config/samples/argocd-health-customizations.yaml)
for an `argocd-cm` customization that maps claim `Ready` conditions to Argo CD
health.

## Troubleshooting

- Check `DatabaseClaim.status.conditions` for CNPG cluster resolution and
  database provisioning errors.
- Check `RoleClaim.status.conditions` for parent `DatabaseClaim`, schema,
  owner-conflict, SQL grant, and Secret errors.
- Check claim events (`kubectl describe`) — failures that cannot land on
  status (e.g. unmigrated legacy claims) are still announced as Warning
  events.
- Check operator logs:

  ```bash
  kubectl logs -n cnpg-dbclaim-system \
    -l app.kubernetes.io/name=dbclaim-operator
  ```

- Verify the referenced CNPG `Cluster` is Ready and that its read-write
  service and superuser Secret exist.
- Verify generated credentials in the `<roleclaim>-credentials` Secret.

For extension install refusals, relocation failures, and lock timeouts, see
[extension-placement.md](extension-placement.md).

## Uninstall

For `deletionPolicy: Retain`, delete dependent `RoleClaim`s before deleting
their `DatabaseClaim`; the operator blocks deletion while roles still refer to
the retained database. For `deletionPolicy: Delete`, deleting the
`DatabaseClaim` cascades through referring `RoleClaim`s and drops the Postgres
database.

Wait for finalizers to clear before removing the operator or CRDs:

```bash
kubectl get databaseclaims,roleclaims -A
```

The Helm chart keeps CRDs on uninstall. Remove them manually only after all
`DatabaseClaim` and `RoleClaim` objects are gone:

```bash
helm uninstall dbclaim-operator -n cnpg-dbclaim-system
kubectl delete crd databaseclaims.cnpg.wyvernzora.io roleclaims.cnpg.wyvernzora.io
```
