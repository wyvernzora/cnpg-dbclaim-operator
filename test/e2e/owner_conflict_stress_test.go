//go:build e2e && e2e_stress

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/controller"
)

var _ = Describe("owner conflict stress", Ordered, Label("stress"), func() {
	It("converges concurrent Owner claims to one deterministic schema owner", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "owner-race")
		dbName := "e2e_ownerrace_" + suffix

		createDBClaim(ctx, ns, "race-db", dbName, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReady(ctx, ns, "race-db")

		createConcurrently(ctx,
			ownerClaim(ns, "left-owner", "race-db", "e2e_owner_left_"+suffix),
			ownerClaim(ns, "right-owner", "race-db", "e2e_owner_right_"+suffix),
		)

		left := getRoleClaim(ctx, ns, "left-owner")
		right := getRoleClaim(ctx, ns, "right-owner")
		expected := deterministicOwner(left, right)

		var ready, conflicted cnpgclaimv1alpha1.RoleClaim
		Eventually(func(g Gomega) {
			left = getRoleClaim(ctx, ns, "left-owner")
			right = getRoleClaim(ctx, ns, "right-owner")
			readyClaims, conflictClaims := classifyOwnerClaims(left, right)
			g.Expect(readyClaims).To(HaveLen(1))
			g.Expect(conflictClaims).To(HaveLen(1))
			ready = readyClaims[0]
			conflicted = conflictClaims[0]
		}, e2eLongTimeout, e2ePollInterval).Should(Succeed())

		Expect(ready.Name).To(Equal(expected.Name), "owner winner should match CreationTimestamp/UID tiebreak")
		Expect(conflicted.Name).NotTo(Equal(expected.Name))

		super, err := connectSuper(ctx, dbName)
		Expect(err).NotTo(HaveOccurred())
		defer super.Close(ctx)
		Eventually(func() string {
			return schemaOwner(ctx, super, "app")
		}, e2eTimeout, e2ePollInterval).Should(Equal(expected.Spec.RoleName))
	})
})

func ownerClaim(ns, name, dbClaim, roleName string) *cnpgclaimv1alpha1.RoleClaim {
	return &cnpgclaimv1alpha1.RoleClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: cnpgclaimv1alpha1.RoleClaimSpec{
			DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: dbClaim},
			RoleName:         roleName,
			Schemas: []cnpgclaimv1alpha1.SchemaGrant{
				{Name: "app", Access: cnpgclaimv1alpha1.AccessOwner},
			},
		},
	}
}

func createConcurrently(ctx context.Context, claims ...*cnpgclaimv1alpha1.RoleClaim) {
	start := make(chan struct{})
	errs := make(chan error, len(claims))
	var wg sync.WaitGroup
	for _, claim := range claims {
		claim := claim
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- k8sClient.Create(ctx, claim)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		Expect(err).NotTo(HaveOccurred())
	}
}

func getRoleClaim(ctx context.Context, ns, name string) cnpgclaimv1alpha1.RoleClaim {
	var got cnpgclaimv1alpha1.RoleClaim
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got)).To(Succeed())
	return got
}

func deterministicOwner(a, b cnpgclaimv1alpha1.RoleClaim) cnpgclaimv1alpha1.RoleClaim {
	if a.CreationTimestamp.Before(&b.CreationTimestamp) {
		return a
	}
	if b.CreationTimestamp.Before(&a.CreationTimestamp) {
		return b
	}
	if string(a.UID) < string(b.UID) {
		return a
	}
	return b
}

func classifyOwnerClaims(claims ...cnpgclaimv1alpha1.RoleClaim) ([]cnpgclaimv1alpha1.RoleClaim, []cnpgclaimv1alpha1.RoleClaim) {
	var ready []cnpgclaimv1alpha1.RoleClaim
	var conflicted []cnpgclaimv1alpha1.RoleClaim
	for _, claim := range claims {
		if claim.Status.Phase == cnpgclaimv1alpha1.RoleClaimPhaseReady {
			ready = append(ready, claim)
			continue
		}
		cond := meta.FindStatusCondition(claim.Status.Conditions, controller.ConditionRoleReady)
		if claim.Status.Phase == cnpgclaimv1alpha1.RoleClaimPhasePending &&
			cond != nil &&
			cond.Reason == controller.ReasonOwnerConflict {
			conflicted = append(conflicted, claim)
		}
	}
	return ready, conflicted
}

func schemaOwner(ctx context.Context, conn *pgx.Conn, schemaName string) string {
	var owner string
	Expect(conn.QueryRow(ctx, `
		SELECT r.rolname
		FROM pg_namespace n
		JOIN pg_roles r ON r.oid = n.nspowner
		WHERE n.nspname = $1
	`, schemaName).Scan(&owner)).To(Succeed())
	return owner
}
