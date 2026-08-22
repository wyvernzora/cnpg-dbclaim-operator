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

#### Extension placement

`CREATE EXTENSION` puts the extension's operator classes and functions in one
schema — every install lands in exactly one, and Postgres has no global
extension. `extensions[].schema` is therefore **required**: the claim names
that schema, and there is no server-default placement and no separate spelling
for "global". An application that pins its runtime `search_path` to a claimed
schema (`search_path=app`) cannot resolve those objects from anywhere else —
`operator class "gist_trgm_ops" does not exist for access method "gist"` is
that failure — so install the extension where its consumers will look.

The schema is a declared desired state. The extension is created with
`CREATE EXTENSION IF NOT EXISTS <name> SCHEMA <schema>`, and an extension
that already exists in a different schema is **converged** onto the declared
one with `ALTER EXTENSION <name> SET SCHEMA <schema>` — the same reconcile
CloudNativePG's own `Database` CRD performs for `extensions[].schema`. The
value must be one of the claim's own `spec.schemas`; a schema outside that
list is rejected at `kubectl apply`, and schemas are created before
extensions are installed, so the target always exists.

An extension is installed before its schema is granted out, never after.
`CREATE EXTENSION` runs the extension's install script as superuser with
`search_path` set to the target schema **plus the schema of every extension that
script's version requires** (`earthdistance` sees `cube`'s schema), so an object
a tenant planted in any of them can capture a name that script resolves.
Reaching the default version can mean running a base script and then a chain of
update scripts, each with its own `requires`, so the operator checks the schema
of every extension **any** available version requires — an extension whose
default version requires nothing is not thereby unguarded. Those schemas are
locked against handover for the duration of the install, extension DDL by other
sessions is blocked for the same window so a prerequisite cannot appear after
the check decided which schemas to look at, and the target is pinned; a schema
that another role owns or holds `CREATE` on is refused rather than installed
into. Declaring the schema and the extension on the same
`DatabaseClaim` is the ordinary path — extensions are installed before any
`RoleClaim` can take the schema, and later reconciles of an already-installed
extension are unaffected by the handover. Three consequences:

- `public` is owned by `pg_database_owner`, which *is* the database owner. An
  explicit `schema: public` works while this operator owns the database (list
  `public` in `spec.schemas` to name it). Once an `Owner` `RoleClaim` takes the
  database, `public` belongs to that tenant, and a **new** extension targeting
  it is refused. Extensions already installed keep reconciling untouched.
- Adding an extension to a schema (or a database) already handed to a tenant
  needs that ownership or `CREATE` grant returned to the superuser for the
  install, or a target schema no `RoleClaim` owns or holds `CREATE` on. A
  `ReadOnly` grant (`USAGE` only) never blocks one.
- Adding an extension whose prerequisite already lives in a tenant-held schema
  is refused too, and picking a different target schema does not help: the
  prerequisite's schema is on the install script's `search_path` either way.
  Relocate the prerequisite to a schema no `RoleClaim` owns or holds `CREATE`
  on, or return that ownership for the install.

The refusal is loud: the claim goes `Failed` with the offending schema and the
reason in the condition message and a Warning event. Nothing is installed.

Those locks are taken with a 10 second `lock_timeout`. A session this operator
does not control can hold the same catalog rows — a tenant that left a
transaction open after its own `CREATE EXTENSION` holds the extension catalog
against writers — and the operator reconciles claims one at a time, so the
install gives up with `canceling statement due to lock timeout` and a Warning
event instead of parking every other claim behind it. The next reconcile
retries.

The same 10 second `lock_timeout` is set on every connection the operator
opens, so the statements outside that transaction — creating the schemas a
claim declares, applying a `RoleClaim`'s role, grants and default privileges —
are bounded the same way and fail the same loud, retried way.

Convergence needs the read-back because `CREATE EXTENSION IF NOT EXISTS`
silently ignores its `SCHEMA` clause when the extension already exists. Some
extensions declare themselves non-relocatable, and Postgres refuses to move
those (`extension "xml2" does not support SET SCHEMA`). That failure is
surfaced, not swallowed: the claim goes `Failed` with
`Reason=ExtensionRelocationFailed` and a Warning event carrying the Postgres
error and both schemas. Resolve it by hand — drop and recreate the extension in
the schema you want, or point the claim at the schema it already occupies.

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
objects, and `schema` is **required** — every extension names the schema its
objects land in. v1alpha1 is the only served and stored version, so claims
written before the upgrade stay *stored* as strings — the API server does not
rewrite them, and the chart's CRDs are ordinary templates that `helm upgrade`
re-applies (`helm.sh/resource-policy: keep` governs uninstall only).

The operator still decodes the old shape, but a stored legacy entry carries no
schema and the operator will not invent one. Plainly:

- An unmigrated claim **fails reconciliation loudly**: every reconcile emits a
  Warning event, and on the provisioning path that event carries a pointer to
  this section. The `Failed` condition cannot even land on the claim — the
  stored strings fail the new schema on every write, status included — so every
  decision the controller records is announced as an event whether or not its
  status write is accepted, and the events plus the controller log are the
  signal. That holds for deletion too: a claim held back by `RoleClaim`
  referrers still reports `BlockedByRoleClaims` as an event.
- An unmigrated claim cannot be edited; the API server rejects any write that
  carries the stored list. It can only be migrated (below) or deleted: on
  deletion the operator releases its finalizer by clearing `spec.extensions`
  with a merge patch, which is safe because deprovisioning uses only
  `databaseName` and `deletionPolicy`.

Migrating means **assigning a schema to every legacy entry** — a decision the
command below cannot make for you. It defaults each entry to the claim's first
`spec.schemas` entry as a starting point; **review every claim and override the
schema per extension before running it** — the right schema is the one the
extension's consumers pin their `search_path` to, and the first list entry is
only a guess. A claim with no `spec.schemas` at all is skipped, because there is
no default to assign. It cannot be given that list on its own, either: every
write against a claim still holding strings carries them along and is rejected
on them, whatever the patch itself says. Migrate such a claim by hand, with one
patch that sets both fields:

```bash
kubectl -n <namespace> patch databaseclaim <name> --type merge \
  -p '{"spec":{"schemas":["app"],"extensions":[{"name":"pgcrypto","schema":"app"}]}}'
```

An explicit `schema: public` works while the operator still owns the database —
list `public` in `spec.schemas` in the same patch.

Every other claim — the ones that do carry a `spec.schemas` — is migrated by
this command:

```bash
kubectl get databaseclaims.cnpg.wyvernzora.io -A -o json \
  | jq -r '.items[] | select(any(.spec.extensions[]?; type == "string"))
      | select((.spec.schemas | length) > 0)
      | .spec.schemas[0] as $default
      | ([.spec.extensions[] | if type == "string" then {name: ., schema: $default} else . end]
         | reduce .[] as $e ([]; if any(.[]; .name == $e.name) then . else . + [$e] end)) as $exts
      | "\(.metadata.namespace) \(.metadata.name) \($exts | tojson)"' \
  | while read -r ns name exts; do
      kubectl -n "$ns" patch databaseclaim "$name" --type merge \
        -p "{\"spec\":{\"extensions\":$exts}}"
    done
```

It selects only claims still holding strings, so it is safe to re-run, and a
JSON merge patch replaces the list wholesale — the one write the API server
accepts against a legacy-shaped object. The `reduce` collapses repeated
extension names to their first occurrence: the old list tolerated repeats, the
new one is keyed by `name`, and a stored list with two identical keys is one a
later apply cannot touch. Two things it cannot do for you: a claim holding more
than 256 extensions exceeds what the new schema accepts and has to be trimmed by
hand first, and the manifests in Git still need updating to the object form —
with a `schema` on every entry, since server-side apply and `kubectl apply`
alike reject an entry without one.

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
