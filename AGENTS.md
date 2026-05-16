# Agent Notes

This repository is a Go/Kubebuilder operator for CloudNativePG-backed
`DatabaseClaim` and `RoleClaim` resources. Prefer small, focused changes that
keep generated manifests, chart templates, and controller RBAC in sync.

## Project Shape

- API types live in `api/v1alpha1`.
- Reconcilers and shared controller helpers live in `internal/controller`.
- SQL and CNPG helpers live in `internal/postgres` and `internal/cnpg`.
- Helm packaging lives in `charts/dbclaim-operator`.
- Kustomize and generated CRDs/RBAC live in `config`.
- End-to-end tests live in `test/e2e` and use kind plus a real CNPG cluster.

## Generated Files

Run `make manifests` after changing RBAC markers, CRD markers, API fields, or
anything that affects `config/crd/bases`, `config/rbac`, or chart CRD templates.
Run `make generate` after changing API types that affect deepcopy output.

The Helm CRD templates are synced by `make manifests`; do not edit
`charts/dbclaim-operator/templates/crds/*.yaml` directly unless you also update
the generator path.

## Verification

Useful local checks:

```bash
env GOCACHE=/private/tmp/cnpg-go-build-cache make test
make chart-template
make chart-lint
env GOCACHE=/private/tmp/cnpg-go-build-cache make e2e-local-clean
```

The explicit `GOCACHE` is needed in the Codex sandbox because the default Go
cache under `~/Library/Caches/go-build` may not be writable. It only changes
where compiled package cache files are stored.

The e2e targets pass `E2E_GINKGO_ARGS` by default so long waits emit Ginkgo
progress reports. Override it, for example `E2E_GINKGO_ARGS=-ginkgo.v`, when a
quieter local run is useful.

`make test` uses envtest and may need permission to bind local loopback ports.
`make e2e-local-clean` creates a kind cluster named `dbclaim-e2e`, builds and
loads the operator image, installs CNPG, runs the e2e suite, and deletes the
cluster via a shell trap when the command exits.

## RBAC And Finalizers

Be careful when tightening RBAC for the primary claim resources:

- `DatabaseClaim` and `RoleClaim` finalizer changes use normal object updates.
- CRDs do not expose a `/finalizers` REST subresource, so
  `client.SubResource("finalizers").Update(...)` returns `NotFound`.
- The normal finalizer update path requires primary-resource `update` RBAC in
  addition to the Kubebuilder `resources=<resource>/finalizers,verbs=update`
  marker.
- `DatabaseClaim` cascade deletion intentionally deletes child `RoleClaim`s, so
  the manager needs `delete` on `roleclaims`.

## CI Gate

PR CI starts with a metadata-only `trust-gate` job. The full suite runs for
Renovate same-repository PRs, PRs opened by owners/members/collaborators,
workflows re-run by actors with write/maintain/admin permission, and PR heads
with an `ok-to-test` commit status. Untrusted PRs fail `trust-gate`, which
should be configured as a required status check so they cannot merge without
maintainer approval.

The `pr-trust-label` workflow runs on `pull_request_target` and must stay
metadata-only: do not check out PR code there. It creates the `ok-to-test`
commit status only after a write/maintain/admin actor adds the label, removes
stale `ok-to-test` labels when a PR receives new commits, and rejects
`ok-to-test` labels added by actors without write, maintain, or admin
permission.

## Commit Style

Match the existing history:

- Use conventional prefixes when practical, for example `feat(controller): ...`,
  `fix(rbac): ...`, `test(e2e): ...`, `chore(deps): ...`, `ci: ...`, and
  `docs: ...`.
- Keep subjects concise, imperative or descriptive, and lower-case after the
  prefix unless naming a proper noun.
- Keep unrelated concerns in separate commits when the branch is headed for PR
  review.
