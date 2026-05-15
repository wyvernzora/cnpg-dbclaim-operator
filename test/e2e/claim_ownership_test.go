//go:build e2e

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/controller"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/postgres"
)

var _ = Describe("claim ownership uniqueness", Ordered, func() {
	It("prevents a newer cross-namespace DatabaseClaim from owning or dropping the same physical database", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		nsA := createNamespace(ctx, "db-owner-a")
		nsB := createNamespace(ctx, "db-owner-b")
		dbName := "e2e_unique_db_" + suffix

		createDBClaim(ctx, nsA, "primary", dbName, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReady(ctx, nsA, "primary")
		createDBClaim(ctx, nsB, "duplicate", dbName, cnpgclaimv1alpha1.DeletionPolicyDelete, "app")
		waitDBPendingReason(ctx, nsB, "duplicate", controller.ReasonDatabaseNameConflict)

		var duplicate cnpgclaimv1alpha1.DatabaseClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "duplicate", Namespace: nsB}, &duplicate)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &duplicate)).To(Succeed())
		Eventually(func() bool {
			var got cnpgclaimv1alpha1.DatabaseClaim
			err := k8sClient.Get(ctx, client.ObjectKey{Name: "duplicate", Namespace: nsB}, &got)
			return apierrors.IsNotFound(err)
		}, e2eLongTimeout, e2ePollInterval).Should(BeTrue())

		super, err := connectSuper(ctx, postgres.AdminDatabase)
		Expect(err).NotTo(HaveOccurred())
		defer super.Close(ctx)
		Eventually(func() bool {
			return databaseExists(ctx, super, dbName)
		}, e2eLongTimeout, e2ePollInterval).Should(BeTrue())
	})

	It("prevents a newer cross-namespace RoleClaim from owning the same physical role on the same cluster", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		nsA := createNamespace(ctx, "role-owner-a")
		nsB := createNamespace(ctx, "role-owner-b")
		roleName := "e2e_unique_role_" + suffix

		createDBClaim(ctx, nsA, "primary-db", "e2e_unique_role_a_"+suffix, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		createDBClaim(ctx, nsB, "other-db", "e2e_unique_role_b_"+suffix, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReady(ctx, nsA, "primary-db")
		waitDBReady(ctx, nsB, "other-db")

		createRoleClaim(ctx, nsA, "primary-role", "primary-db", roleName, cnpgclaimv1alpha1.AccessReadWrite)
		waitRoleReady(ctx, nsA, "primary-role")
		createRoleClaim(ctx, nsB, "duplicate-role", "other-db", roleName, cnpgclaimv1alpha1.AccessReadOnly)
		waitRolePendingReason(ctx, nsB, "duplicate-role", controller.ReasonRoleNameConflict)

		secret := getSecret(ctx, nsA, "primary-role-credentials")
		Expect(string(secret.Data["user"])).To(Equal(roleName))
		var duplicateClaim cnpgclaimv1alpha1.RoleClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "duplicate-role", Namespace: nsB}, &duplicateClaim)).To(Succeed())
		Expect(duplicateClaim.Status.CredentialsSecretName).To(BeEmpty())
		var duplicateSecret corev1.Secret
		err := k8sClient.Get(ctx, client.ObjectKey{Name: "duplicate-role-credentials", Namespace: nsB}, &duplicateSecret)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected no duplicate credentials Secret, got %v", err)
	})
})
