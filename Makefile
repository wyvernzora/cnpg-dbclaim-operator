# cnpg-dbclaim-operator Makefile

IMG ?= cnpg-dbclaim-operator:dev
GO ?= go
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
KUSTOMIZE ?= $(LOCALBIN)/kustomize
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint

LOCALBIN ?= $(shell pwd)/bin

CONTROLLER_TOOLS_VERSION ?= v0.16.5
KUSTOMIZE_VERSION ?= v5.4.3
ENVTEST_K8S_VERSION ?= 1.31.0
GOLANGCI_LINT_VERSION ?= v1.61.0

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
manifests: controller-gen ## Generate CRD and RBAC manifests.
	$(CONTROLLER_GEN) crd rbac:roleName=manager-role webhook paths=./... output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

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
