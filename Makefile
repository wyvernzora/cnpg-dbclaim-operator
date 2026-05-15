# cnpg-dbclaim-operator Makefile

CNPG_VERSION ?= 1.27.4
CNPG_MINOR ?= 1.27
IMAGE_REPOSITORY ?= cnpg-dbclaim-operator
IMAGE_TAG ?= e2e
IMG ?= $(IMAGE_REPOSITORY):$(IMAGE_TAG)
E2E_STRESS_COUNT ?= 10
GO ?= go
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
KUSTOMIZE ?= $(LOCALBIN)/kustomize
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint

LOCALBIN ?= $(shell pwd)/bin

CONTROLLER_TOOLS_VERSION ?= v0.16.5
KUSTOMIZE_VERSION ?= v5.4.3
ENVTEST_K8S_VERSION ?= 1.31.0
GOLANGCI_LINT_VERSION ?= v1.64.8

.PHONY: all
all: build

.PHONY: help
help:
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-25s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

##@ Development

.PHONY: generate
generate: controller-gen ## Generate DeepCopy methods.
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...

.PHONY: manifests
manifests: controller-gen ## Generate CRD and RBAC manifests and sync into the chart.
	$(CONTROLLER_GEN) crd rbac:roleName=manager-role webhook paths=./... output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac
	@mkdir -p charts/dbclaim-operator/templates/crds
	@for src in config/crd/bases/cnpg.wyvernzora.io_databaseclaims.yaml config/crd/bases/cnpg.wyvernzora.io_roleclaims.yaml; do \
		base=$$(basename $$src | sed -e 's|cnpg.wyvernzora.io_||' -e 's|s.yaml|.yaml|'); \
		dst=charts/dbclaim-operator/templates/crds/$$base; \
		{ echo '{{- if .Values.installCRDs -}}'; cat $$src; echo '{{- end }}'; } | \
		sed 's|controller-gen.kubebuilder.io/version: v0.16.5|controller-gen.kubebuilder.io/version: v0.16.5\n    helm.sh/resource-policy: keep|' > $$dst; \
	done

.PHONY: fmt
fmt: ## Run go fmt.
	$(GO) fmt ./...

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet ./...

.PHONY: lint
lint: golangci-lint ## Run golangci-lint.
	$(GOLANGCI_LINT) run

.PHONY: test
test: manifests generate fmt vet envtest ## Run unit and envtest-based integration tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		$(GO) test ./... -coverprofile cover.out

.PHONY: build
build: manifests generate fmt vet ## Build the manager binary.
	$(GO) build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	$(GO) run ./cmd/main.go

##@ Build

.PHONY: docker-build
docker-build: ## Build the operator container image.
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push: ## Push the operator container image.
	docker push $(IMG)

##@ End-to-end

.PHONY: verify-cnpg-version
verify-cnpg-version: ## Verify Makefile CNPG_VERSION matches go.mod.
	@grep -q "github.com/cloudnative-pg/cloudnative-pg v$(CNPG_VERSION)" go.mod || \
	  { echo "go.mod CNPG version != Makefile CNPG_VERSION ($(CNPG_VERSION))"; exit 1; }

.PHONY: kind-up
kind-up: ## Create the local kind cluster used by e2e.
	kind create cluster --config hack/e2e/kind-config.yaml --name dbclaim-e2e

.PHONY: kind-down
kind-down: ## Delete the local kind cluster used by e2e.
	kind delete cluster --name dbclaim-e2e

.PHONY: kind-load
kind-load: ## Load the operator image into the e2e kind cluster.
	kind load docker-image $(IMG) --name dbclaim-e2e

.PHONY: cnpg-install
cnpg-install: ## Install the pinned CloudNativePG operator into the current cluster.
	kubectl apply --server-side -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-$(CNPG_MINOR)/releases/cnpg-$(CNPG_VERSION).yaml
	kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=300s

.PHONY: cluster-up
cluster-up: ## Create the shared e2e Postgres Cluster.
	kubectl apply -f hack/e2e/test-cluster.yaml
	kubectl -n cnpg-system wait --for=condition=Ready clusters.postgresql.cnpg.io/shared-pg --timeout=300s

.PHONY: operator-install
operator-install: ## Install the operator chart into the current cluster.
	helm upgrade --install dbclaim charts/dbclaim-operator \
	  --namespace cnpg-dbclaim-system --create-namespace \
	  --set image.repository=$(IMAGE_REPOSITORY) \
	  --set image.tag=$(IMAGE_TAG) \
	  --set image.pullPolicy=Never \
	  --set installCRDs=true \
	  --set leaderElection=false
	kubectl -n cnpg-dbclaim-system rollout status deployment/dbclaim-dbclaim-operator --timeout=180s

.PHONY: e2e
e2e: ## Run the kind/CNPG end-to-end suite against the current cluster.
	$(GO) test -tags=e2e -count=1 -v -timeout 15m ./test/e2e/...

.PHONY: e2e-stress
e2e-stress: ## Run the opt-in e2e stress suite against the current cluster.
	$(GO) test -tags='e2e e2e_stress' -count=$(E2E_STRESS_COUNT) -v -timeout 60m ./test/e2e/...

.PHONY: e2e-local
e2e-local: kind-up docker-build kind-load cnpg-install cluster-up operator-install e2e ## Bring up kind and run e2e, leaving the cluster for debugging.

.PHONY: e2e-local-clean
e2e-local-clean: ## Bring up kind, run e2e, and always delete the cluster.
	trap 'kind delete cluster --name dbclaim-e2e' EXIT; \
	  $(MAKE) e2e-local

##@ Tooling

.PHONY: controller-gen
controller-gen: $(LOCALBIN)
	@test -x $(CONTROLLER_GEN) || GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: kustomize
kustomize: $(LOCALBIN)
	@test -x $(KUSTOMIZE) || GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

.PHONY: envtest
envtest: $(LOCALBIN)
	@test -x $(ENVTEST) || GOBIN=$(LOCALBIN) $(GO) install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.19

.PHONY: golangci-lint
golangci-lint: $(LOCALBIN)
	@test -x $(GOLANGCI_LINT) || GOBIN=$(LOCALBIN) $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(LOCALBIN):
	@mkdir -p $(LOCALBIN)
