/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

// legacyDatabaseClaimCRD is the shape v0.3.x served: spec.extensions is a list
// of bare strings with no cap. Objects created against it stay stored that way
// after the CRD is upgraded, because v1alpha1 is the only served and stored
// version and the API server does not rewrite objects at rest.
const legacyDatabaseClaimCRD = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: databaseclaims.cnpg.wyvernzora.io
spec:
  group: cnpg.wyvernzora.io
  names:
    kind: DatabaseClaim
    listKind: DatabaseClaimList
    plural: databaseclaims
    singular: databaseclaim
  scope: Namespaced
  versions:
    - name: v1alpha1
      served: true
      storage: true
      subresources:
        status: {}
      schema:
        openAPIV3Schema:
          type: object
          properties:
            apiVersion:
              type: string
            kind:
              type: string
            metadata:
              type: object
            spec:
              type: object
              required: [databaseName, clusterRef]
              properties:
                databaseName:
                  type: string
                clusterRef:
                  type: object
                  required: [name, namespace]
                  properties:
                    name:
                      type: string
                    namespace:
                      type: string
                extensions:
                  type: array
                  x-kubernetes-list-type: atomic
                  items:
                    type: string
                schemas:
                  type: array
                  x-kubernetes-list-type: set
                  items:
                    type: string
                deletionPolicy:
                  type: string
            status:
              type: object
              x-kubernetes-preserve-unknown-fields: true
`

// TestFinalizerReleaseOnLegacyStoredClaim proves a claim written by v0.3.x can
// still be deleted after the CRD is upgraded, at an extension count no old
// claim was stopped from reaching.
//
// With spec.extensions[].schema required, the ordinary finalizer release — a
// whole-object update — is wedged on an unmigrated legacy claim: it re-encodes
// the stored bare strings into objects that carry no schema, and the API
// server rejects the write as Invalid. CRD validation ratcheting does not
// exempt the stored strings either (they fail the item schema outright), so no
// full-object write can succeed. The test asserts that wedge explicitly, then
// asserts the deletion fallback in updateFinalizers — a JSON merge patch that
// writes the remaining finalizers and clears spec.extensions — releases the
// claim.
//
// It also pins the status-write behaviour on an unmigrated claim: the status
// subresource replaces the request's spec with the stored one, but the
// spec-level CEL rule still evaluates against the stored bare strings and
// validation ratcheting does not exempt it, so even status writes are
// rejected. The Failed condition never lands on a legacy claim — the Warning
// event is its loud reconcile-failure signal, which is why the apply-error
// branch in reconcileNormal emits the event even when its status write fails.
func TestFinalizerReleaseOnLegacyStoredClaim(t *testing.T) {
	ctx := context.Background()
	c, upgradeCRDs := startLegacyEnv(t)

	// More than a v0.3.x claim was ever stopped from holding, and well past the
	// 32 the new shape first shipped with.
	extensions := make([]any, 0, 200)
	for i := range 200 {
		extensions = append(extensions, legacyExtensionName(i))
	}
	legacy := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "cnpg.wyvernzora.io/v1alpha1",
		"kind":       "DatabaseClaim",
		"metadata": map[string]any{
			"name":       "legacy",
			"namespace":  "default",
			"finalizers": []any{DatabaseClaimFinalizer},
		},
		"spec": map[string]any{
			"databaseName": "legacy",
			"clusterRef":   map[string]any{"name": "pg", "namespace": "default"},
			"extensions":   extensions,
		},
	}}
	if err := c.Create(ctx, legacy); err != nil {
		t.Fatalf("create legacy claim: %v", err)
	}

	upgradeCRDs(ctx)

	var claim cnpgclaimv1alpha1.DatabaseClaim
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "legacy"}, &claim); err != nil {
		t.Fatalf("typed get of a legacy-stored claim: %v", err)
	}
	if len(claim.Spec.Extensions) != 200 || claim.Spec.Extensions[0].Name != legacyExtensionName(0) {
		t.Fatalf("decoded extensions = %v, want the 200 legacy names as objects", claim.Spec.Extensions)
	}

	// The loud-failure surface: measured on this API server (1.31), a status
	// write on an unmigrated claim is rejected too — the status subresource
	// replaces the request's spec with the stored one, but the spec-level CEL
	// rule fails against the stored bare strings and ratcheting does not
	// exempt it. The Failed condition therefore cannot land on a legacy
	// claim, and the Warning event carries the reconcile failure instead.
	statusClaim := claim.DeepCopy()
	statusClaim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseFailed
	setCondition(&statusClaim.Status.Conditions, statusClaim.Generation,
		ConditionReady, metav1.ConditionFalse, ReasonReconcileFailed, "extension has no schema")
	if err := c.Status().Update(ctx, statusClaim); !apierrors.IsInvalid(err) {
		t.Fatalf("status write on an unmigrated legacy claim: got err %v, want Invalid (see comment)", err)
	}

	// The wedge: a whole-object update re-encodes the stored strings into
	// objects without the now-required schema, and nothing in the request can
	// supply one, so the ordinary finalizer-release write is rejected.
	wedged := claim.DeepCopy()
	controllerutil.RemoveFinalizer(wedged, DatabaseClaimFinalizer)
	if err := c.Update(ctx, wedged); !apierrors.IsInvalid(err) {
		t.Fatalf("whole-object finalizer release on a legacy-stored claim: got err %v, want Invalid", err)
	}

	// The way out: delete the claim, and let updateFinalizers fall back to the
	// merge patch that clears spec.extensions alongside the finalizers.
	if err := c.Delete(ctx, legacy); err != nil {
		t.Fatalf("delete legacy claim: %v", err)
	}
	var deleting cnpgclaimv1alpha1.DatabaseClaim
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "legacy"}, &deleting); err != nil {
		t.Fatalf("get deleting claim: %v", err)
	}
	if deleting.DeletionTimestamp.IsZero() {
		t.Fatal("claim has no deletionTimestamp after delete; the finalizer should be holding it")
	}
	controllerutil.RemoveFinalizer(&deleting, DatabaseClaimFinalizer)
	if err := updateFinalizers(ctx, c, &deleting); err != nil {
		t.Fatalf("release finalizer on a deleting legacy-stored claim: %v", err)
	}

	// With the last finalizer gone the API server finishes the deletion.
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(legacy.GroupVersionKind())
	err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "legacy"}, stored)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("get after finalizer release: got err %v (finalizers %v), want NotFound", err, stored.GetFinalizers())
	}
}

// TestLegacyStoredClaimMigrationWrites pins the two write shapes the "Upgrading
// from v0.3.x" section depends on.
//
// A claim stored with an empty extensions list asks for no extension, so it has
// no schema to declare and nothing to migrate: it must stay writable without a
// spec.schemas, or the operator error-loops on a claim whose spec requests
// nothing. A claim still holding bare strings and no spec.schemas cannot be
// given that list on its own — the stored strings ride along in the request and
// fail the item schema — so schemas and extensions have to land in one patch.
func TestLegacyStoredClaimMigrationWrites(t *testing.T) {
	ctx := context.Background()
	c, upgradeCRDs := startLegacyEnv(t)

	newLegacyClaim := func(name string, extensions []any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "cnpg.wyvernzora.io/v1alpha1",
			"kind":       "DatabaseClaim",
			"metadata":   map[string]any{"name": name, "namespace": "default"},
			"spec": map[string]any{
				"databaseName": name,
				"clusterRef":   map[string]any{"name": "pg", "namespace": "default"},
				"extensions":   extensions,
			},
		}}
	}
	for _, claim := range []*unstructured.Unstructured{
		newLegacyClaim("legacy-empty", []any{}),
		newLegacyClaim("legacy-strings", []any{"pgcrypto", "pg_trgm"}),
	} {
		if err := c.Create(ctx, claim); err != nil {
			t.Fatalf("create %s: %v", claim.GetName(), err)
		}
	}

	upgradeCRDs(ctx)

	// Nothing to migrate, so nothing may be wedged: the reconcile loop's
	// success-path status write has to land on this claim.
	var emptyClaim cnpgclaimv1alpha1.DatabaseClaim
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "legacy-empty"}, &emptyClaim); err != nil {
		t.Fatalf("get claim stored with an empty extensions list: %v", err)
	}
	emptyClaim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseReady
	setCondition(&emptyClaim.Status.Conditions, emptyClaim.Generation,
		ConditionReady, metav1.ConditionTrue, ReasonProvisioned, "")
	if err := c.Status().Update(ctx, &emptyClaim); err != nil {
		t.Fatalf("status write on a claim stored with an empty extensions list: %v", err)
	}

	// The step the README used to tell operators to take on its own.
	key := client.ObjectKey{Namespace: "default", Name: "legacy-strings"}
	target := &cnpgclaimv1alpha1.DatabaseClaim{}
	if err := c.Get(ctx, key, target); err != nil {
		t.Fatalf("get claim stored with bare strings: %v", err)
	}
	schemasOnly := []byte(`{"spec":{"schemas":["app"]}}`)
	err := c.Patch(ctx, target.DeepCopy(), client.RawPatch(types.MergePatchType, schemasOnly))
	// Rejected on the stored strings the patch never mentions, which is why no
	// amount of retrying the schemas-only write can make it land.
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.extensions[0] in body must be of type object") {
		t.Fatalf("schemas-only patch on a legacy-stored claim: got err %v, want Invalid on the stored strings", err)
	}

	// The step that works: one patch carrying both.
	combined := []byte(`{"spec":{"schemas":["app"],"extensions":[{"name":"pgcrypto","schema":"app"},{"name":"pg_trgm","schema":"app"}]}}`)
	if err := c.Patch(ctx, target.DeepCopy(), client.RawPatch(types.MergePatchType, combined)); err != nil {
		t.Fatalf("combined schemas+extensions patch on a legacy-stored claim: %v", err)
	}
	var migrated cnpgclaimv1alpha1.DatabaseClaim
	if err := c.Get(ctx, key, &migrated); err != nil {
		t.Fatalf("get migrated claim: %v", err)
	}
	want := []cnpgclaimv1alpha1.ExtensionSpec{
		{Name: "pgcrypto", Schema: "app"},
		{Name: "pg_trgm", Schema: "app"},
	}
	if !reflect.DeepEqual(migrated.Spec.Extensions, want) || !reflect.DeepEqual(migrated.Spec.Schemas, []string{"app"}) {
		t.Fatalf("migrated spec = %+v / %v, want %+v / [app]", migrated.Spec.Extensions, migrated.Spec.Schemas, want)
	}
}

// startLegacyEnv brings up an API server serving the v0.3.x CRD, so a test can
// create objects in the shape v0.3.x left stored. The returned function
// upgrades the CRD in place — exactly as `helm upgrade` re-applies it — and
// blocks until the API server validates against the new one.
func startLegacyEnv(t *testing.T) (client.Client, func(context.Context)) {
	t.Helper()

	_, file, _, _ := runtime.Caller(0)
	projectRoot := filepath.Join(filepath.Dir(file), "..", "..")

	legacyDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(legacyDir, "databaseclaim.yaml"), []byte(legacyDatabaseClaimCRD), 0o600); err != nil {
		t.Fatalf("write legacy CRD: %v", err)
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{legacyDir},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	s := k8sruntime.NewScheme()
	if err := cnpgclaimv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatalf("build client: %v", err)
	}

	return c, func(ctx context.Context) {
		if _, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
			Paths: []string{filepath.Join(projectRoot, "config", "crd", "bases")},
		}); err != nil {
			t.Fatalf("install upgraded CRDs: %v", err)
		}
		waitForObjectFormAccepted(ctx, t, c)
	}
}

func legacyExtensionName(i int) string {
	return "ext_" + string(rune('a'+i/26)) + string(rune('a'+i%26))
}

// waitForObjectFormAccepted blocks until the upgraded schema is the one the API
// server validates against, so the test cannot pass by racing ahead of it.
func waitForObjectFormAccepted(ctx context.Context, t *testing.T, c client.Client) {
	t.Helper()
	probe := &cnpgclaimv1alpha1.DatabaseClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "schema-probe", Namespace: "default"},
		Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
			DatabaseName: "probe",
			ClusterRef:   cnpgclaimv1alpha1.ClusterReference{Name: "pg", Namespace: "default"},
			Schemas:      []string{"app"},
			Extensions:   []cnpgclaimv1alpha1.ExtensionSpec{{Name: "pgcrypto", Schema: "app"}},
		},
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := c.Create(ctx, probe)
		if err == nil {
			if err := c.Delete(ctx, probe); err != nil {
				t.Fatalf("delete schema probe: %v", err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("upgraded schema never took effect: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
