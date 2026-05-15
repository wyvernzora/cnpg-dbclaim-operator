//go:build e2e

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/controller"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/postgres"
)

var _ = Describe("deletion policy", Ordered, func() {
	It("cascades RoleClaims and drops the database for deletionPolicy=Delete", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "delete")
		dbName := "e2e_delete_" + suffix
		roleName := "e2e_delete_role_" + suffix

		createDBClaim(ctx, ns, "delete-db", dbName, cnpgclaimv1alpha1.DeletionPolicyDelete, "app")
		waitDBReady(ctx, ns, "delete-db")
		createRoleClaim(ctx, ns, "delete-role", "delete-db", roleName, cnpgclaimv1alpha1.AccessReadWrite)
		waitRoleReady(ctx, ns, "delete-role")

		var dbClaim cnpgclaimv1alpha1.DatabaseClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "delete-db", Namespace: ns}, &dbClaim)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &dbClaim)).To(Succeed())

		Eventually(func() bool {
			var role cnpgclaimv1alpha1.RoleClaim
			err := k8sClient.Get(ctx, client.ObjectKey{Name: "delete-role", Namespace: ns}, &role)
			return apierrors.IsNotFound(err)
		}, e2eLongTimeout, e2ePollInterval).Should(BeTrue())
		Eventually(func() bool {
			var claim cnpgclaimv1alpha1.DatabaseClaim
			err := k8sClient.Get(ctx, client.ObjectKey{Name: "delete-db", Namespace: ns}, &claim)
			return apierrors.IsNotFound(err)
		}, e2eLongTimeout, e2ePollInterval).Should(BeTrue())

		super, err := connectSuper(ctx, postgres.AdminDatabase)
		Expect(err).NotTo(HaveOccurred())
		defer super.Close(ctx)
		Eventually(func() bool {
			return databaseExists(ctx, super, dbName)
		}, e2eLongTimeout, e2ePollInterval).Should(BeFalse())
	})

	It("blocks deletionPolicy=Retain while RoleClaims refer to it and preserves the database", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "retain")
		dbName := "e2e_retain_" + suffix
		roleName := "e2e_retain_role_" + suffix

		createDBClaim(ctx, ns, "retain-db", dbName, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReady(ctx, ns, "retain-db")
		createRoleClaim(ctx, ns, "retain-role", "retain-db", roleName, cnpgclaimv1alpha1.AccessReadWrite)
		waitRoleReady(ctx, ns, "retain-role")

		var dbClaim cnpgclaimv1alpha1.DatabaseClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "retain-db", Namespace: ns}, &dbClaim)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &dbClaim)).To(Succeed())
		waitDBDeletingReason(ctx, ns, "retain-db", controller.ReasonBlockedByRoleClaims)

		var role cnpgclaimv1alpha1.RoleClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "retain-role", Namespace: ns}, &role)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &role)).To(Succeed())

		Eventually(func() bool {
			var got cnpgclaimv1alpha1.RoleClaim
			err := k8sClient.Get(ctx, client.ObjectKey{Name: "retain-role", Namespace: ns}, &got)
			return apierrors.IsNotFound(err)
		}, e2eLongTimeout, e2ePollInterval).Should(BeTrue())
		Eventually(func() bool {
			var claim cnpgclaimv1alpha1.DatabaseClaim
			err := k8sClient.Get(ctx, client.ObjectKey{Name: "retain-db", Namespace: ns}, &claim)
			return apierrors.IsNotFound(err)
		}, e2eLongTimeout, e2ePollInterval).Should(BeTrue())

		super, err := connectSuper(ctx, postgres.AdminDatabase)
		Expect(err).NotTo(HaveOccurred())
		defer super.Close(ctx)
		Eventually(func() bool {
			return databaseExists(ctx, super, dbName)
		}, e2eLongTimeout, e2ePollInterval).Should(BeTrue())
	})
})
