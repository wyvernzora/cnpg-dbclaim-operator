//go:build e2e

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"context"
	"fmt"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

var _ = Describe("owner deletion", Ordered, func() {
	It("does not let a deleting Owner sibling block a successor Owner", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "owner-delete")
		dbName := "e2e_ownerdelete_" + suffix
		oldRole := "e2e_owner_old_" + suffix
		newRole := "e2e_owner_new_" + suffix
		holdFinalizer := "e2e.cnpg.wyvernzora.io/hold"

		createDBClaim(ctx, ns, "owners-db", dbName, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReady(ctx, ns, "owners-db")
		createRoleClaim(ctx, ns, "old-owner", "owners-db", oldRole, cnpgclaimv1alpha1.AccessOwner)
		waitRoleReady(ctx, ns, "old-owner")

		var old cnpgclaimv1alpha1.RoleClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "old-owner", Namespace: ns}, &old)).To(Succeed())
		before := old.DeepCopy()
		old.Finalizers = append(old.Finalizers, holdFinalizer)
		Expect(k8sClient.Patch(ctx, &old, client.MergeFrom(before))).To(Succeed())
		Expect(k8sClient.Delete(ctx, &old)).To(Succeed())

		Eventually(func(g Gomega) {
			var deleting cnpgclaimv1alpha1.RoleClaim
			g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "old-owner", Namespace: ns}, &deleting)).To(Succeed())
			g.Expect(deleting.DeletionTimestamp.IsZero()).To(BeFalse())
		}, e2eTimeout, e2ePollInterval).Should(Succeed())

		createRoleClaim(ctx, ns, "new-owner", "owners-db", newRole, cnpgclaimv1alpha1.AccessOwner)
		newOwner := waitRoleReady(ctx, ns, "new-owner")
		conn, err := connectAs(ctx, getSecret(ctx, ns, newOwner.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close(ctx)
		expectExec(ctx, conn, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", quoteTable("app", "successor_items")))

		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "old-owner", Namespace: ns}, &old)).To(Succeed())
		before = old.DeepCopy()
		old.Finalizers = slices.DeleteFunc(old.Finalizers, func(f string) bool { return f == holdFinalizer })
		Expect(k8sClient.Patch(ctx, &old, client.MergeFrom(before))).To(Succeed())
	})
})
