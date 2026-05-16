/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	cnpgresolver "github.com/wyvernzora/cnpg-dbclaim-operator/internal/cnpg"
)

// ensureNamespace creates a Namespace if it doesn't already exist. envtest does
// not auto-create namespaces other than default, so cross-namespace specs need
// to plant the target namespace themselves.
func ensureNamespace(ctx context.Context, name string) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8sClient.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

// makeHealthyCluster creates a CNPG Cluster with the given allowlist
// annotation (omit by passing ""), its companion superuser Secret, and forces
// Status.Phase to Healthy so cnpgresolver.Resolve succeeds. The Postgres
// endpoint won't actually answer — these specs only exercise the gating that
// happens before any SQL is attempted.
func makeHealthyCluster(ctx context.Context, name, namespace, allowlist string) {
	makeHealthyClusterWithSecret(ctx, name, namespace, allowlist, true)
}

func makeHealthyClusterWithSecret(ctx context.Context, name, namespace, allowlist string, createSecret bool) {
	ensureNamespace(ctx, namespace)

	if createSecret {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-superuser", Namespace: namespace},
			Data:       map[string][]byte{"username": []byte("postgres"), "password": []byte("pw")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
	}

	annotations := map[string]string{}
	if allowlist != "" {
		annotations[cnpgresolver.ClaimAllowlistAnnotation] = allowlist
	}
	cluster := &cnpgv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Annotations: annotations},
		Spec:       cnpgv1.ClusterSpec{Instances: 1},
	}
	Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

	Eventually(func() error {
		var c cnpgv1.Cluster
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, &c); err != nil {
			return err
		}
		c.Status.Phase = cnpgv1.PhaseHealthy
		return k8sClient.Status().Update(ctx, &c)
	}, 10*time.Second, 250*time.Millisecond).Should(Succeed())
}

var _ = Describe("Cluster claim allowlist", func() {
	It("denies a cross-namespace DatabaseClaim when the cluster's allowlist excludes it", func() {
		ctx := context.Background()
		suffix := fmt.Sprintf("%d", time.Now().UnixNano())
		clusterNs := "infra-" + suffix
		clusterName := "pg-" + suffix
		makeHealthyCluster(ctx, clusterName, clusterNs, "team-x,team-y")

		claim := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "denied-" + suffix, Namespace: "default"},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "denied_db",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: clusterName, Namespace: clusterNs},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), claim)
		})

		Eventually(func() []string {
			var got cnpgclaimv1alpha1.DatabaseClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: claim.Name, Namespace: "default"}, &got); err != nil {
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
		}, 20*time.Second, 500*time.Millisecond).Should(Equal([]string{ReasonClaimNotAllowed, ReasonClaimNotAllowed}))

		Eventually(func() int32 {
			return eventCount(ctx, "default", "DatabaseClaim", claim.Name, ReasonClaimNotAllowed, corev1.EventTypeWarning)
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(int32(1)))
	})

	It("denies a cross-namespace DatabaseClaim before requiring the cluster superuser Secret", func() {
		ctx := context.Background()
		suffix := fmt.Sprintf("%d", time.Now().UnixNano())
		clusterNs := "infra-nosecret-" + suffix
		clusterName := "pg-nosecret-" + suffix
		makeHealthyClusterWithSecret(ctx, clusterName, clusterNs, "team-x", false)

		claim := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "denied-nosecret-" + suffix, Namespace: "default"},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "denied_nosecret_db",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: clusterName, Namespace: clusterNs},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), claim)
		})

		Eventually(func() string {
			var got cnpgclaimv1alpha1.DatabaseClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: claim.Name, Namespace: "default"}, &got); err != nil {
				return err.Error()
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(ReasonClaimNotAllowed))
	})

	It("denies a RoleClaim when the cluster's allowlist excludes its namespace", func() {
		ctx := context.Background()
		suffix := fmt.Sprintf("%d", time.Now().UnixNano())
		clusterNs := "infra-rc-" + suffix
		clusterName := "pg-rc-" + suffix
		makeHealthyCluster(ctx, clusterName, clusterNs, "team-x")

		db := &cnpgclaimv1alpha1.DatabaseClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "rc-deny-db-" + suffix, Namespace: "default"},
			Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
				DatabaseName:   "rc_denied_db",
				ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: clusterName, Namespace: clusterNs},
				Schemas:        []string{"app"},
				DeletionPolicy: cnpgclaimv1alpha1.DeletionPolicyRetain,
			},
		}
		Expect(k8sClient.Create(ctx, db)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), db)
		})
		// Force the parent DBClaim Ready so the RoleClaim controller proceeds
		// past parent-readiness into the cluster resolve / allowlist gate.
		forceDBClaimReady(ctx, db.Name, "default")

		access := cnpgclaimv1alpha1.AccessReadWrite
		rc := &cnpgclaimv1alpha1.RoleClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "rc-deny-" + suffix, Namespace: "default"},
			Spec: cnpgclaimv1alpha1.RoleClaimSpec{
				DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: db.Name},
				RoleName:         "rc_denied_role",
				Access:           &access,
			},
		}
		Expect(k8sClient.Create(ctx, rc)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(context.Background(), rc)
		})

		Eventually(func() string {
			forceDBClaimReady(ctx, db.Name, "default")
			var got cnpgclaimv1alpha1.RoleClaim
			if err := k8sClient.Get(ctx, client.ObjectKey{Name: rc.Name, Namespace: "default"}, &got); err != nil {
				return err.Error()
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
			if cond == nil {
				return ""
			}
			return cond.Reason
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(ReasonClaimNotAllowed))

		Eventually(func() int32 {
			return eventCount(ctx, "default", "RoleClaim", rc.Name, ReasonClaimNotAllowed, corev1.EventTypeWarning)
		}, 20*time.Second, 500*time.Millisecond).Should(Equal(int32(1)))
	})
})
