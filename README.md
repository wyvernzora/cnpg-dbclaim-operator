# cnpg-dbclaim-operator

Provision [CloudNativePG](https://cloudnative-pg.io/)-backed databases and roles
via two composable, namespaced Kubernetes custom resources: **DatabaseClaim**
and **RoleClaim**.

App teams submit Claims in their own namespace. The operator runs as a
cluster-singleton, resolves the referenced CNPG `Cluster`, opens a superuser
SQL session against its read-write endpoint, and idempotently creates the
database, schemas, extensions, roles, grants, and reflexive default
privileges. Each `RoleClaim` emits a Kubernetes `Secret` with the role's
credentials.

## Why

CNPG already ships `Cluster` (cluster lifecycle) and `Database` (per-DB
lifecycle) custom resources, but role provisioning lives in
`Cluster.spec.managed.roles[]`. Embedding role config in the Cluster spec
forces app teams to PR into the platform team's manifest every time they
need a credential. This operator turns that around: cluster ownership stays
with the platform team; each app team submits Claims in its own namespace.

## Resources

### DatabaseClaim

Owns a Postgres database. Locks down the `public` schema, ensures named
schemas and extensions exist, and waits for an `Owner` `RoleClaim` to take
ownership.

```yaml
apiVersion: cnpg.wyvernzora.io/v1alpha1
kind: DatabaseClaim
metadata: { name: orders, namespace: app-team-a }
spec:
  databaseName: orders
  clusterRef:
    name: shared-pg
    namespace: cnpg-system
  schemas: [app]
  extensions: [pgcrypto]
  deletionPolicy: Retain          # Retain (default) | Delete
```

### RoleClaim

Provisions a single Postgres role with a permission profile, and writes a
credential `Secret` alongside.

`spec.roleName` is **required and immutable**: the Kubernetes resource name
commonly contains `-`, which is not a valid Postgres identifier, so there is
no safe implicit default. Set it explicitly to a value matching
`^[a-z][a-z0-9_]{0,62}$`.

Sugar form (single-app pattern):

```yaml
apiVersion: cnpg.wyvernzora.io/v1alpha1
kind: RoleClaim
metadata: { name: orders-rw, namespace: app-team-a }
spec:
  databaseClaimRef: { name: orders }
  roleName: orders_rw
  access: ReadWrite               # Owner | ReadWrite | ReadOnly
```

Per-schema form (bounded-context pattern):

```yaml
apiVersion: cnpg.wyvernzora.io/v1alpha1
kind: RoleClaim
metadata: { name: ordering-svc, namespace: app-team-a }
spec:
  databaseClaimRef: { name: orders-domain }
  roleName: ordering_svc
  schemas:
    - { name: ordering,    access: Owner    }
    - { name: shared,      access: ReadWrite }
    - { name: shipping,    access: ReadOnly }
    - { name: fulfillment, access: ReadOnly }
```

The resulting `Secret` (named `<roleclaim>-credentials`) carries the
following keys: `host`, `port`, `dbname`, `user`, `password`, `uri` (libpq
URI), `jdbc_uri`.

Physical Postgres resources have one winning claim per CNPG cluster. If two
`DatabaseClaim`s target the same `(clusterRef, databaseName)`, or two
`RoleClaim`s target the same Postgres role name on the same cluster, the
oldest claim wins. Later duplicates stay `Pending` with
`Reason=DatabaseNameConflict` or `Reason=RoleNameConflict` and do not touch
SQL state or credentials.

## Choosing the right shape

| Situation | Pattern |
|---|---|
| Single app owns a database | One `DatabaseClaim` + RoleClaims using sugar `spec.access:` |
| Microservices share data within a bounded context (cross-schema reads/joins) | One `DatabaseClaim` + a `RoleClaim` per service using per-schema `spec.schemas[]` |
| Co-deployed apps with independent data | One `DatabaseClaim` per app on the same `Cluster` |
| Apps with different availability or PG-version requirements | Separate CNPG `Cluster`s (out of scope for this operator) |

## How it works

The operator runs a superuser SQL session against the CNPG cluster's
read-write service. The CNPG Cluster CR is observed read-only — the
operator never patches it, so there is no contention with the GitOps tool
that owns the cluster manifest.

Default privileges are reconciled reflexively: when any RoleClaim becomes
Ready, the operator walks all sibling RoleClaims that share a schema and
issues `ALTER DEFAULT PRIVILEGES` for every writer/reader pair. Future
objects created by an `Owner` or `ReadWrite` writer are visible to
`ReadOnly` readers, and writable by `ReadWrite` readers, on the same schema,
without explicit configuration — provided migrations are run as a writer
role.

DatabaseClaim deletion is conservative:
- `deletionPolicy: Retain` (default) blocks deletion while any `RoleClaim`
  references the claim. The status reports `Reason=BlockedByRoleClaims`
  with the names of blockers.
- `deletionPolicy: Delete` cascades through referring `RoleClaim`s and then
  drops the database (`pg_terminate_backend` + `DROP DATABASE`).

## Installation

Install CloudNativePG before installing this operator. The operator expects a
ready CNPG `Cluster` and uses the cluster-generated superuser credentials to
provision databases and roles.

### Helm

```bash
helm install dbclaim-operator \
  oci://ghcr.io/wyvernzora/charts/dbclaim-operator \
  --version 0.2.0 \
  --namespace cnpg-dbclaim-system \
  --create-namespace
```

CRDs are installed by the chart (templates/crds/) with
`helm.sh/resource-policy: keep` so they survive an uninstall. Pass
`--set installCRDs=false` to skip if you manage CRDs out of band.

To install from a local checkout instead:

```bash
helm install dbclaim-operator charts/dbclaim-operator \
  --namespace cnpg-dbclaim-system \
  --create-namespace \
  --values charts/dbclaim-operator/values.example.yaml
```

### Kustomize

```bash
kustomize build config/default | kubectl apply -f -
```

The Kustomize tree assumes the operator image is published as
`cnpg-dbclaim-operator:latest`; override via a kustomize image transformer
in your overlay.

## Sample scenarios

See `config/samples/`:
- `scenario_a_simple.yaml` — single app, sugar form
- `scenario_b_bounded_context.yaml` — bounded-context shared DB
- `scenario_c_independent_apps.yaml` — separate DBs on one cluster

After applying claims, verify readiness through status conditions:

```bash
kubectl get databaseclaims,roleclaims -A
kubectl describe databaseclaim -n app-team-a orders
kubectl describe roleclaim -n app-team-a orders-rw
kubectl get secret -n app-team-a orders-rw-credentials -o yaml
```

## Operations

### Argo CD health checks

Argo CD's `Synced` status only means the live Kubernetes object matches Git; it
does not know whether this operator reconciled external Postgres state. See
[config/samples/argocd-health-customizations.yaml](config/samples/argocd-health-customizations.yaml)
for an `argocd-cm` customization that maps claim `Ready` conditions to Argo CD
health.

### Troubleshooting

- Check `DatabaseClaim.status.conditions` for CNPG cluster resolution and
  database provisioning errors.
- Check `RoleClaim.status.conditions` for parent `DatabaseClaim`, schema,
  owner-conflict, SQL grant, and Secret errors.
- Check operator logs:

  ```bash
  kubectl logs -n cnpg-dbclaim-system \
    -l app.kubernetes.io/name=dbclaim-operator
  ```

- Verify the referenced CNPG `Cluster` is Ready and that its read-write
  service and superuser Secret exist.
- Verify generated credentials in the `<roleclaim>-credentials` Secret.

### Uninstall

For `deletionPolicy: Retain`, delete dependent `RoleClaim`s before deleting
their `DatabaseClaim`; the operator blocks deletion while roles still refer
to the retained database. For `deletionPolicy: Delete`, deleting the
`DatabaseClaim` cascades through referring `RoleClaim`s and drops the
Postgres database.

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

## Development

```bash
make manifests   # regenerate CRDs/RBAC (also syncs into Helm chart)
make build       # compile manager binary into bin/
make test        # unit + envtest-based integration tests
make chart-lint  # lint the Helm chart
make docker-build IMG=cnpg-dbclaim-operator:dev
```

The operator builds against Go 1.25+; the toolchain version is pinned in
`go.mod`. `golangci-lint` is used for static checks (`make lint`).

## Status

v1alpha1 — API may evolve. Out of scope for v1: password rotation,
extension version updates, schema drop on removal from spec,
per-table-pattern grants.
