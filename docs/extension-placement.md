# Extension placement

`DatabaseClaim.spec.extensions[]` entries are `{name, schema}` objects, and
`schema` is **required**. This document explains why, what the operator does
to converge placement, the security model behind install refusals, and how to
resolve each failure.

## Why schema is required

`CREATE EXTENSION` puts the extension's operator classes and functions in one
schema — every install lands in exactly one, and Postgres has no global
extension. The claim therefore names that schema; there is no server-default
placement and no separate spelling for "global". An application that pins its
runtime `search_path` to a claimed schema (`search_path=app`) cannot resolve
those objects from anywhere else — `operator class "gist_trgm_ops" does not
exist for access method "gist"` is that failure — so install the extension
where its consumers will look.

The value must be one of the claim's own `spec.schemas`; a schema outside that
list is rejected at `kubectl apply`, and schemas are created before extensions
are installed, so the target always exists.

## Declared placement and convergence

The schema is a declared desired state. The extension is created with
`CREATE EXTENSION IF NOT EXISTS <name> SCHEMA <schema>`, and an extension that
already exists in a different schema is **converged** onto the declared one
with `ALTER EXTENSION <name> SET SCHEMA <schema>` — the same reconcile
CloudNativePG's own `Database` CRD performs for `extensions[].schema`.

Convergence needs a read-back because `CREATE EXTENSION IF NOT EXISTS`
silently ignores its `SCHEMA` clause when the extension already exists. Some
extensions declare themselves non-relocatable, and Postgres refuses to move
those (`extension "xml2" does not support SET SCHEMA`). That failure is
surfaced, not swallowed: the claim goes `Failed` with
`Reason=ExtensionRelocationFailed` and a Warning event carrying the Postgres
error and both schemas. Resolve it by hand — drop and recreate the extension
in the schema you want, or point the claim at the schema it already occupies.

## Install refusals: the security model

An extension is installed before its schema is granted out, never after.
`CREATE EXTENSION` runs the extension's install script as superuser with
`search_path` set to the target schema **plus the schema of every extension
that script's version requires** (`earthdistance` sees `cube`'s schema), so an
object a tenant planted in any of them can capture a name that script
resolves. Reaching the default version can mean running a base script and then
a chain of update scripts, each with its own `requires`, so the operator
checks the schema of every extension **any** available version requires — an
extension whose default version requires nothing is not thereby unguarded.
Those schemas are locked against handover for the duration of the install,
extension DDL by other sessions is blocked for the same window so a
prerequisite cannot appear after the check decided which schemas to look at,
and the target is pinned; a schema that another role owns or holds `CREATE` on
is refused rather than installed into.

Declaring the schema and the extension on the same `DatabaseClaim` is the
ordinary path — extensions are installed before any `RoleClaim` can take the
schema, and later reconciles of an already-installed extension are unaffected
by the handover. Three consequences:

- `public` is owned by `pg_database_owner`, which *is* the database owner. An
  explicit `schema: public` works while this operator owns the database (list
  `public` in `spec.schemas` to name it). Once an `Owner` `RoleClaim` takes
  the database, `public` belongs to that tenant, and a **new** extension
  targeting it is refused. Extensions already installed keep reconciling
  untouched.
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

## Lock timeouts

The install's locks are taken with a 10 second `lock_timeout`. A session this
operator does not control can hold the same catalog rows — a tenant that left
a transaction open after its own `CREATE EXTENSION` holds the extension
catalog against writers — and the operator reconciles claims one at a time, so
the install gives up with `canceling statement due to lock timeout` and a
Warning event instead of parking every other claim behind it. The next
reconcile retries.

The same 10 second `lock_timeout` is set on every connection the operator
opens, so the statements outside that transaction — creating the schemas a
claim declares, applying a `RoleClaim`'s role, grants and default privileges —
are bounded the same way and fail the same loud, retried way.
