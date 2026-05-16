//go:build e2e

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/controller"
)

var _ = Describe("cluster allowlist", Ordered, func() {
	It("denies cross-namespace DatabaseClaims until the namespace is allowed", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespaceWithoutClusterAccess(ctx, "allowlist")

		createDBClaim(ctx, ns, "blocked-db", "e2e_allowlist_"+suffix, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBPendingReason(ctx, ns, "blocked-db", controller.ReasonClaimNotAllowed)

		allowClaimNamespace(ctx, ns)
		waitDBReady(ctx, ns, "blocked-db")
	})
})
