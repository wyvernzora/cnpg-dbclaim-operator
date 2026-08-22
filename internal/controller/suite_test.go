/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

const cnpgModulePath = "github.com/cloudnative-pg/cloudnative-pg"

var (
	testEnv    *envtest.Environment
	cfg        *rest.Config
	k8sClient  client.Client
	mgrCancel  context.CancelFunc
	mgrStarted bool
)

// locateCNPGCRDs returns the path to the directory containing the CNPG
// Cluster CRD in the local module cache. We need at least the Cluster CRD
// installed in envtest so the controller-runtime watch on Cluster registers
// successfully.
//
// The path is discovered via `go list -m` rather than hardcoded so that a
// go.mod bump doesn't silently break the test setup.
func locateCNPGCRDs() (string, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", cnpgModulePath)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("resolve %s module dir: %w", cnpgModulePath, err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		return "", fmt.Errorf("module %s not found in module graph", cnpgModulePath)
	}
	return filepath.Join(dir, "config", "crd", "bases"), nil
}

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	_, file, _, _ := runtime.Caller(0)
	projectRoot := filepath.Join(filepath.Dir(file), "..", "..")

	cnpgCRDs, locErr := locateCNPGCRDs()
	Expect(locErr).NotTo(HaveOccurred())

	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join(projectRoot, "config", "crd", "bases"),
			cnpgCRDs,
		},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	utilruntime.Must(cnpgclaimv1alpha1.AddToScheme(scheme.Scheme))
	utilruntime.Must(cnpgv1.AddToScheme(scheme.Scheme))

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	startManagerOnce()
})

var _ = AfterSuite(func() {
	if mgrCancel != nil {
		mgrCancel()
	}
	if testEnv != nil {
		Expect(testEnv.Stop()).To(Succeed())
	}
})

// startManagerOnce launches a single manager + both reconcilers shared across
// every spec in this suite. controller-runtime rejects duplicate controller
// names on the same metrics registry, so per-spec managers are not workable.
func startManagerOnce() {
	if mgrStarted {
		return
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	Expect(err).NotTo(HaveOccurred())

	Expect(SetupFieldIndexes(context.Background(), mgr)).To(Succeed())
	Expect((&DatabaseClaimReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr)).To(Succeed())
	Expect((&RoleClaimReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr)).To(Succeed())

	ctx, cancel := context.WithCancel(context.Background())
	mgrCancel = cancel
	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()
	mgrStarted = true
}

// Ensure a freshly-created DatabaseClaim that references a missing Cluster
// transitions to Pending with ClusterResolved=False (no panics, finalizer
// added) within a reasonable time.
var _ = Describe("DatabaseClaim", func() {
	const ns = "default"

	It("goes Pending with ClusterMissing when the Cluster doesn't exist", func() {
		ctx := context.Background()
		claim := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "missing-cluster", Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "demo",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: "no-such-cluster", Namespace: ns},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), claim)
		})

		Eventually(func() []string {
			var got cnpgclaimv1alpha1.DatabaseClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: claim.Name, Namespace: ns}, &got); err != nil {
				if apierrors.IsNotFound(err) {
					return []string{"notfound", "notfound"}
				}
				return []string{err.Error(), err.Error()}
			}
			clusterCond := meta.FindStatusCondition(got.Status.Conditions, ConditionClusterResolved)
			readyCond := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
			reasons := []string{"", ""}
			if clusterCond != nil {
				reasons[0] = clusterCond.Reason
			}
			if readyCond != nil {
				reasons[1] = readyCond.Reason
			}
			return reasons
		}, 20*time.Second, 500*time.Millisecond).Should(Equal([]string{ReasonClusterMissing, ReasonClusterMissing}))

		Eventually(func() int32 {
			return eventCount(ctx, ns, "DatabaseClaim", claim.Name, ReasonClusterMissing, corev1.EventTypeWarning)
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(int32(1)))
		Consistently(func() int32 {
			return eventCount(ctx, ns, "DatabaseClaim", claim.Name, ReasonClusterMissing, corev1.EventTypeWarning)
		}, 17*time.Second, 500*time.Millisecond).Should(Equal(int32(1)))
	})

	It("goes Pending with ClusterNotReady when the Cluster exists but is not ready", func() {
		ctx := context.Background()
		clusterName := fmt.Sprintf("not-ready-%d", time.Now().UnixNano())
		cluster := &cnpgv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns},
			Spec:       cnpgv1.ClusterSpec{Instances: 1},
		}
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), cluster)
		})

		claim := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-db", Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "demo_not_ready",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: clusterName, Namespace: ns},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), claim)
		})

		Eventually(func() string {
			var got cnpgclaimv1alpha1.DatabaseClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: claim.Name, Namespace: ns}, &got); err != nil {
				return err.Error()
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(ReasonClusterNotReady))
	})

	It("marks the newer duplicate physical database claim as conflicted", func() {
		ctx := context.Background()
		suffix := fmt.Sprintf("%d", time.Now().UnixNano())
		clusterRef := cnpgclaimv1alpha1.ClusterReference{Name: "missing-" + suffix, Namespace: ns}
		older := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "db-owner-a-" + suffix, Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "same_db_" + suffix,
				ClusterRef:     clusterRef,
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		newer := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "db-owner-b-" + suffix, Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   older.Spec.DatabaseName,
				ClusterRef:     clusterRef,
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		Expect(k8sClient.Create(ctx, older)).To(Succeed())
		time.Sleep(1100 * time.Millisecond)
		Expect(k8sClient.Create(ctx, newer)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), older)
			_ = k8sClient.Delete(context.Background(), newer)
		})

		Eventually(func() string {
			var got cnpgclaimv1alpha1.DatabaseClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: newer.Name, Namespace: newer.Namespace}, &got); err != nil {
				return err.Error()
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(ReasonDatabaseNameConflict))

		Eventually(func() int32 {
			return eventCount(ctx, ns, "DatabaseClaim", newer.Name, ReasonDatabaseNameConflict, corev1.EventTypeWarning)
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(int32(1)))
	})

	It("persists the schema requested for an extension", func() {
		ctx := context.Background()
		name := fmt.Sprintf("ext-schema-%d", time.Now().UnixNano())
		claim := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "demo_ext_schema",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: "no-such-cluster", Namespace: ns},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
				Schemas:        []string{"releases"},
				Extensions: []cnpgclaimv1alpha1.ExtensionSpec{
					{Name: "pg_trgm", Schema: "releases"},
					{Name: "pgcrypto", Schema: "releases"},
				},
			},
		}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), claim)
		})

		var got cnpgclaimv1alpha1.DatabaseClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got)).To(Succeed())
		Expect(got.Spec.Extensions).To(Equal([]cnpgclaimv1alpha1.ExtensionSpec{
			{Name: "pg_trgm", Schema: "releases"},
			{Name: "pgcrypto", Schema: "releases"},
		}))
	})

	It("rejects an extension entry that does not name a schema", func() {
		ctx := context.Background()
		claim := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ext-no-schema-%d", time.Now().UnixNano()), Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "demo_ext_no_schema",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: "no-such-cluster", Namespace: ns},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
				Schemas:        []string{"releases"},
				Extensions:     []cnpgclaimv1alpha1.ExtensionSpec{{Name: "pg_trgm"}},
			},
		}
		err := k8sClient.Create(ctx, claim)
		if err == nil {
			_ = k8sClient.Delete(context.Background(), claim)
		}
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "create without extension schema: got err %v, want Invalid", err)
		Expect(err.Error()).To(ContainSubstring("Required value"))
	})

	// An empty list asks for no extension at all, so there is no schema for it
	// to declare. A v0.3.x claim can be stored that way, and the rule has to
	// accept it or every write to that claim — status writes included — is
	// rejected for a spec that requests nothing. Unstructured because the Go
	// type's omitempty drops an empty slice, so the typed client cannot spell
	// the shape at all.
	It("accepts an empty extensions list with no spec.schemas", func() {
		ctx := context.Background()
		name := fmt.Sprintf("ext-empty-%d", time.Now().UnixNano())
		claim := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "cnpg.wyvernzora.io/v1alpha1",
			"kind":       "DatabaseClaim",
			"metadata":   map[string]any{"name": name, "namespace": ns},
			"spec": map[string]any{
				"databaseName": "demo_ext_empty",
				"clusterRef":   map[string]any{"name": "no-such-cluster", "namespace": ns},
				"extensions":   []any{},
			},
		}}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), claim)
		})
	})

	It("rejects an extension whose schema is not declared in spec.schemas", func() {
		ctx := context.Background()
		cases := map[string][]string{
			"schemas omits the extension schema": {"app"},
			"schemas absent entirely":            nil,
		}
		for label, schemas := range cases {
			By(label, func() {
				claim := &cnpgclaimv1alpha1.DatabaseClaim{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ext-undeclared-%d", time.Now().UnixNano()), Namespace: ns},
					Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
						DatabaseName:   "demo_ext_undeclared",
						ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: "no-such-cluster", Namespace: ns},
						DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
						Schemas:        schemas,
						Extensions:     []cnpgclaimv1alpha1.ExtensionSpec{{Name: "pg_trgm", Schema: "releases"}},
					},
				}
				err := k8sClient.Create(ctx, claim)
				if err == nil {
					_ = k8sClient.Delete(context.Background(), claim)
				}
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "create with schemas %v: got err %v, want Invalid", schemas, err)
				Expect(err.Error()).To(ContainSubstring("each extension schema must be declared in spec.schemas"))
			})
		}
	})

	It("rejects extension entries that are not safe Postgres identifiers", func() {
		ctx := context.Background()
		cases := map[string]cnpgclaimv1alpha1.ExtensionSpec{
			"bad name":   {Name: "pg-trgm", Schema: "releases"},
			"bad schema": {Name: "pg_trgm", Schema: "Releases"},
			"empty name": {Schema: "releases"},
		}
		for label, ext := range cases {
			By(label, func() {
				claim := &cnpgclaimv1alpha1.DatabaseClaim{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("ext-invalid-%d", time.Now().UnixNano()), Namespace: ns},
					Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
						DatabaseName:   "demo_ext_invalid",
						ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: "no-such-cluster", Namespace: ns},
						DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
						// Declared so that entries with a well-formed schema
						// fail on the identifier pattern, not the
						// schema-must-be-declared rule.
						Schemas:    []string{"releases"},
						Extensions: []cnpgclaimv1alpha1.ExtensionSpec{ext},
					},
				}
				err := k8sClient.Create(ctx, claim)
				if err == nil {
					_ = k8sClient.Delete(context.Background(), claim)
				}
				Expect(apierrors.IsInvalid(err)).To(BeTrue(), "create with extension %+v: got err %v, want Invalid", ext, err)
			})
		}
	})
})

var _ = Describe("RoleClaim", func() {
	const ns = "default"

	It("goes Pending with DatabaseClaimMissing when the parent is absent", func() {
		ctx := context.Background()
		access := cnpgclaimv1alpha1.AccessReadWrite
		rc := &cnpgclaimv1alpha1.RoleClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: ns},
			Spec: cnpgclaimv1alpha1.RoleClaimSpec{
				DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: "missing"},
				RoleName:         "orphan_role",
				Access:           &access,
			},
		}
		Expect(k8sClient.Create(ctx, rc)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), rc)
		})

		Eventually(func() string {
			var got cnpgclaimv1alpha1.RoleClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: rc.Name, Namespace: ns}, &got); err != nil {
				return err.Error()
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, ConditionDatabaseClaimResolved)
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(ReasonDatabaseClaimMissing))

		Eventually(func() int32 {
			return eventCount(ctx, ns, "RoleClaim", rc.Name, ReasonDatabaseClaimMissing, corev1.EventTypeWarning)
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(int32(1)))
	})

	It("rejects creation without spec.roleName", func() {
		ctx := context.Background()
		access := string(cnpgclaimv1alpha1.AccessReadOnly)
		rc := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "cnpg.wyvernzora.io/v1alpha1",
				"kind":       "RoleClaim",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("missing-role-name-%d", time.Now().UnixNano()),
					"namespace": ns,
				},
				"spec": map[string]any{
					"databaseClaimRef": map[string]any{"name": "missing"},
					"access":           access,
				},
			},
		}
		err := k8sClient.Create(ctx, rc)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected invalid error, got %v", err)
	})

	It("rejects updates that change spec.roleName", func() {
		ctx := context.Background()
		access := cnpgclaimv1alpha1.AccessReadOnly
		name := fmt.Sprintf("immutable-role-name-%d", time.Now().UnixNano())
		rc := &cnpgclaimv1alpha1.RoleClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: cnpgclaimv1alpha1.RoleClaimSpec{
				DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: "missing"},
				RoleName:         "original_role",
				Access:           &access,
			},
		}
		Expect(k8sClient.Create(ctx, rc)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), rc)
		})

		Eventually(func() bool {
			var got cnpgclaimv1alpha1.RoleClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got); err != nil {
				return false
			}
			got.Spec.RoleName = "changed_role"
			err := k8sClient.Update(ctx, &got)
			return apierrors.IsInvalid(err)
		}, 10*time.Second, 250*time.Millisecond).Should(BeTrue())
	})

	It("rejects updates that replace spec.schemas with spec.access", func() {
		ctx := context.Background()
		name := fmt.Sprintf("immutable-access-%d", time.Now().UnixNano())
		rc := &cnpgclaimv1alpha1.RoleClaim{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: cnpgclaimv1alpha1.RoleClaimSpec{
				DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: "missing"},
				RoleName:         "schema_role",
				Schemas: []cnpgclaimv1alpha1.SchemaGrant{
					{Name: "app", Access: cnpgclaimv1alpha1.AccessReadOnly},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rc)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), rc)
		})

		Eventually(func() bool {
			var got cnpgclaimv1alpha1.RoleClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got); err != nil {
				return false
			}
			access := cnpgclaimv1alpha1.AccessReadOnly
			got.Spec.Schemas = nil
			got.Spec.Access = &access
			err := k8sClient.Update(ctx, &got)
			return apierrors.IsInvalid(err)
		}, 10*time.Second, 250*time.Millisecond).Should(BeTrue())
	})

	It("marks the newer duplicate physical role claim as conflicted before creating a Secret", func() {
		ctx := context.Background()
		suffix := fmt.Sprintf("%d", time.Now().UnixNano())
		clusterRef := cnpgclaimv1alpha1.ClusterReference{Name: "missing-" + suffix, Namespace: ns}
		dbA := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "role-db-a-" + suffix, Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "role_db_a_" + suffix,
				ClusterRef:     clusterRef,
				Schemas:        []string{"app"},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		dbB := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "role-db-b-" + suffix, Namespace: ns},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "role_db_b_" + suffix,
				ClusterRef:     clusterRef,
				Schemas:        []string{"app"},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		Expect(k8sClient.Create(ctx, dbA)).To(Succeed())
		Expect(k8sClient.Create(ctx, dbB)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), dbA)
			_ = k8sClient.Delete(context.Background(), dbB)
		})
		forceDBClaimReady(ctx, dbA.Name, ns)
		forceDBClaimReady(ctx, dbB.Name, ns)

		access := cnpgclaimv1alpha1.AccessReadWrite
		older := &cnpgclaimv1alpha1.RoleClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "role-owner-a-" + suffix, Namespace: ns},
			Spec: cnpgclaimv1alpha1.RoleClaimSpec{
				DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: dbA.Name},
				RoleName:         "same_role_" + suffix,
				Access:           &access,
			},
		}
		newer := &cnpgclaimv1alpha1.RoleClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "role-owner-b-" + suffix, Namespace: ns},
			Spec: cnpgclaimv1alpha1.RoleClaimSpec{
				DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: dbB.Name},
				RoleName:         older.Spec.RoleName,
				Access:           &access,
			},
		}
		Expect(k8sClient.Create(ctx, older)).To(Succeed())
		time.Sleep(1100 * time.Millisecond)
		Expect(k8sClient.Create(ctx, newer)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), older)
			_ = k8sClient.Delete(context.Background(), newer)
		})

		Eventually(func() string {
			forceDBClaimReady(ctx, dbA.Name, ns)
			forceDBClaimReady(ctx, dbB.Name, ns)
			var got cnpgclaimv1alpha1.RoleClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: newer.Name, Namespace: newer.Namespace}, &got); err != nil {
				return err.Error()
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, ConditionRoleReady)
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(ReasonRoleNameConflict))

		var secret corev1.Secret
		err := k8sClient.Get(ctx, client.ObjectKey{Name: newer.Name + "-credentials", Namespace: ns}, &secret)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected no credentials Secret, got %v", err)
	})
})

func forceDBClaimReady(ctx context.Context, name, namespace string) {
	Eventually(func() error {
		var claim cnpgclaimv1alpha1.DatabaseClaim
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &claim); err != nil {
			return err
		}
		claim.Status.Phase = cnpgclaimv1alpha1.DatabaseClaimPhaseReady
		claim.Status.ObservedGeneration = claim.Generation
		setCondition(&claim.Status.Conditions, claim.Generation, ConditionReady, metav1.ConditionTrue, ReasonProvisioned, "")
		return k8sClient.Status().Update(ctx, &claim)
	}, 10*time.Second, 250*time.Millisecond).Should(Succeed())
}

func eventCount(ctx context.Context, namespace, kind, name, reason, eventType string) int32 {
	var events corev1.EventList
	if err := k8sClient.List(ctx, &events, client.InNamespace(namespace)); err != nil {
		return -1
	}
	var count int32
	for _, event := range events.Items {
		if event.InvolvedObject.Kind != kind ||
			event.InvolvedObject.Name != name ||
			event.Reason != reason ||
			event.Type != eventType {
			continue
		}
		if event.Count == 0 {
			count++
			continue
		}
		count += event.Count
	}
	return count
}
