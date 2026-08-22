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
  extensions:
    - name: pgcrypto
      schema: app                 # objects created in schema "app"
    - name: pg_trgm
      schema: app
  deletionPolicy: Retain          # Retain (default) | Delete
```

`extensions[].schema` is **required** — Postgres has no global extension, so
every install names the one schema its objects land in, and the value must be
one of the claim's own `spec.schemas`. An extension that already exists in a
different schema is converged onto the declared one with `ALTER EXTENSION ...
SET SCHEMA`. Installs into a schema a tenant role owns or can write to are
refused, and refusals, relocation failures, and lock timeouts all surface as a
`Failed` condition with a Warning event. See
[docs/extension-placement.md](docs/extension-placement.md) for the full
placement semantics, the security model behind the refusals, and how to
resolve each failure.

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

### Cluster opt-in (allowlist)

Cluster owners control which tenant namespaces may target their CNPG
`Cluster` via an annotation. Cross-namespace claims are **denied by
default**; claims in the same namespace as the `Cluster` are always allowed.

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: shared-pg
  namespace: cnpg-system
  annotations:
    cnpg.wyvernzora.io/allowed-claim-namespaces: app-team-a,app-team-b
```

Claims from a namespace not listed (and not same-namespace) park in
`Pending` with `Reason=ClaimNotAllowed`. Editing the annotation triggers a
reconcile on every referencing claim. The check applies only to the
provisioning path; deletion always proceeds so finalizers cannot get
stranded by a later allowlist tightening.

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
  --namespace cnpg-dbclaim-system \
  --create-namespace
```

This takes the latest published chart, which is the one this README documents.
Pin a specific chart with `--version <x.y.z>` if you need a reproducible
install — but note that `spec.extensions` changed shape across releases, so a
pin older than the API described above will reject the manifests here (see
[Upgrading from v0.3.x](#upgrading-from-v03x)).

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

### Upgrading from v0.3.x

`spec.extensions` changed from a list of strings to a list of `{name, schema}`
objects, and `schema` is now required. Claims written before the upgrade stay
stored in the old shape: they fail reconciliation loudly (Warning events; the
`Failed` condition cannot land on them) and cannot be edited — only migrated
or deleted. See [docs/upgrading-from-v0.3.md](docs/upgrading-from-v0.3.md) for
the full behavior and the one-time migration commands.

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

Argo CD health mapping, troubleshooting, and uninstall ordering are covered in
[docs/operations.md](docs/operations.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup, build and
test targets, and how to run the Postgres-gated integration tests.

## Status

v1alpha1 — API may evolve. Out of scope for v1: password rotation,
extension version updates, schema drop on removal from spec,
per-table-pattern grants.
