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
issues `ALTER DEFAULT PRIVILEGES FOR ROLE <writer> IN SCHEMA <s> GRANT
SELECT ... TO <reader>` for every (writer, reader) pair. This means future
objects created by any writer become visible to all readers, on the same
schema, without explicit configuration — provided migrations are run as a
writer role.

DatabaseClaim deletion is conservative:
- `deletionPolicy: Retain` (default) blocks deletion while any `RoleClaim`
  references the claim. The status reports `Reason=BlockedByRoleClaims`
  with the names of blockers.
- `deletionPolicy: Delete` cascades through referring `RoleClaim`s and then
  drops the database (`pg_terminate_backend` + `DROP DATABASE`).

## Installation

### Helm

```bash
helm install dbclaim-operator charts/dbclaim-operator \
  --namespace cnpg-system \
  --create-namespace \
  --set image.repository=ghcr.io/wyvernzora/cnpg-dbclaim-operator \
  --set image.tag=v0.1.0
```

CRDs are installed by the chart (templates/crds/) with
`helm.sh/resource-policy: keep` so they survive an uninstall. Pass
`--set installCRDs=false` to skip if you manage CRDs out of band.

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

## Development

```bash
make manifests   # regenerate CRDs/RBAC (also syncs into Helm chart)
make build       # compile manager binary into bin/
make test        # unit + envtest-based integration tests
make docker-build IMG=cnpg-dbclaim-operator:dev
```

The operator builds against Go 1.25+; the toolchain version is pinned in
`go.mod`. `golangci-lint` is used for static checks (`make lint`).

## Status

v1alpha1 — API may evolve. Out of scope for v1: password rotation,
extension version updates, schema drop on removal from spec,
per-table-pattern grants.
