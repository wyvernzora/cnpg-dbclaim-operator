/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"sigs.k8s.io/yaml"
)

// crdShape is the minimal OpenAPI shape we need to assert against. We only
// look at the required list under spec.versions[0].schema.openAPIV3Schema.
// Field tags are nested map traversals; sigs.k8s.io/yaml decodes JSON-aliased
// YAML into structs and maps interchangeably.
type crdShape struct {
	Spec struct {
		Versions []struct {
			Schema struct {
				OpenAPIV3Schema struct {
					Properties struct {
						Spec struct {
							Required []string `json:"required,omitempty"`
						} `json:"spec"`
					} `json:"properties"`
				} `json:"openAPIV3Schema"`
			} `json:"schema"`
		} `json:"versions"`
	} `json:"spec"`
}

// TestRoleClaimCRDRequiresRoleName guards against a controller-gen regression
// in which spec.roleName loses its required status. The field has no implicit
// default — making it optional silently re-enables the bug where K8s names
// containing '-' get spliced into Postgres identifiers and fail at reconcile.
func TestRoleClaimCRDRequiresRoleName(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	path := filepath.Join(root, "config", "crd", "bases", "cnpg.wyvernzora.io_roleclaims.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read CRD %s: %v", path, err)
	}
	var shape crdShape
	if err := yaml.Unmarshal(data, &shape); err != nil {
		t.Fatalf("parse CRD %s: %v", path, err)
	}
	if len(shape.Spec.Versions) == 0 {
		t.Fatalf("CRD has no versions")
	}
	required := shape.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec.Required
	if !slices.Contains(required, "roleName") {
		t.Fatalf("RoleClaim CRD spec.required does not contain \"roleName\"; got %v", required)
	}
}
