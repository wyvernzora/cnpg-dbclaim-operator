# Upgrading from v0.3.x

`spec.extensions` changed from a list of strings to a list of `{name, schema}`
objects, and `schema` is **required** — every extension names the schema its
objects land in (see [extension-placement.md](extension-placement.md)).
v1alpha1 is the only served and stored version, so claims written before the
upgrade stay *stored* as strings — the API server does not rewrite them, and
the chart's CRDs are ordinary templates that `helm upgrade` re-applies
(`helm.sh/resource-policy: keep` governs uninstall only).

## What happens to unmigrated claims

The operator still decodes the old shape, but a stored legacy entry carries no
schema and the operator will not invent one. Plainly:

- An unmigrated claim **fails reconciliation loudly**: every reconcile emits a
  Warning event, and on the provisioning path that event carries a pointer to
  this guide. The `Failed` condition cannot even land on the claim — the
  stored strings fail the new schema on every write, status included — so
  every decision the controller records is announced as an event whether or
  not its status write is accepted, and the events plus the controller log are
  the signal. That holds for deletion too: a claim held back by `RoleClaim`
  referrers still reports `BlockedByRoleClaims` as an event.
- An unmigrated claim cannot be edited; the API server rejects any write that
  carries the stored list. It can only be migrated (below) or deleted: on
  deletion the operator releases its finalizer by clearing `spec.extensions`
  with a merge patch, which is safe because deprovisioning uses only
  `databaseName` and `deletionPolicy`.

## Migrating

Migrating means **assigning a schema to every legacy entry** — a decision the
command below cannot make for you. It defaults each entry to the claim's first
`spec.schemas` entry as a starting point; **review every claim and override
the schema per extension before running it** — the right schema is the one the
extension's consumers pin their `search_path` to, and the first list entry is
only a guess.

A claim with no `spec.schemas` at all is skipped, because there is no default
to assign. It cannot be given that list on its own, either: every write
against a claim still holding strings carries them along and is rejected on
them, whatever the patch itself says. Migrate such a claim by hand, with one
patch that sets both fields:

```bash
kubectl -n <namespace> patch databaseclaim <name> --type merge \
  -p '{"spec":{"schemas":["app"],"extensions":[{"name":"pgcrypto","schema":"app"}]}}'
```

An explicit `schema: public` works while the operator still owns the
database — list `public` in `spec.schemas` in the same patch.

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
later apply cannot touch. Two things it cannot do for you: a claim holding
more than 256 extensions exceeds what the new schema accepts and has to be
trimmed by hand first, and the manifests in Git still need updating to the
object form — with a `schema` on every entry, since server-side apply and
`kubectl apply` alike reject an entry without one.
