# Contributing

## Prerequisites

- Go 1.25+ (the toolchain version is pinned in `go.mod`)
- Docker (integration tests against real Postgres; kind-based e2e)
- `make` — all tooling (controller-gen, envtest, golangci-lint, kustomize) is
  installed into `bin/` by the Makefile on first use

## Build, test, lint

```bash
make manifests   # regenerate CRDs/RBAC (syncs CRDs into the Helm chart;
                 # the chart's rbac.yaml is hand-maintained — keep it in step)
make build       # compile manager binary into bin/
make test        # unit + envtest-based integration tests
make lint        # golangci-lint
make chart-lint  # lint the Helm chart
make docker-build IMG=cnpg-dbclaim-operator:dev
```

`make test` regenerates manifests first; a dirty `git status` after it means a
generated file was committed stale.

## Postgres-gated integration tests

The tests in `internal/postgres` that exercise real SQL behavior are gated on
`CNPG_DBCLAIM_POSTGRES_DSN` and skip without it. To run them:

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:17
export CNPG_DBCLAIM_POSTGRES_DSN='postgres://postgres:pw@127.0.0.1:5432/postgres?sslmode=disable'
go test ./internal/postgres/
```

A few fixture-based tests additionally need the server's extension directory
writable by the postgres process; they skip loudly with the exact `docker
exec ... chmod` to run when it is not.

## Git hooks and commit messages

Run once after cloning:

```bash
lefthook install
```

This wires the `commit-msg` hooks in `.githooks/`: commit subjects must be
[Conventional Commits](https://www.conventionalcommits.org/)
(`<type>[(<scope>)][!]: <description>`, ≤ 72 chars, types
`feat|fix|docs|refactor|test|build|ci|chore|perf|revert`, scope optional), and
commit messages must not carry AI-assistant attribution. CI (`ci-lint.yml`)
enforces the same subject rules on PR titles and pushed commits.

## Pull requests

Run `make test` and `make lint` before opening a PR. Keep generated artifacts
(`config/crd/bases/`, `config/rbac/role.yaml`, chart CRDs) in sync with the
markers that produce them — `make test` regenerates them, so a stale copy
shows up as an uncommitted diff.
