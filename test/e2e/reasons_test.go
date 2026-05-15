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

var _ = Describe("ready reason mapping", Ordered, func() {
	It("reports ClusterMissing for a missing referenced CNPG cluster", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "reasons-missing")

		createDBClaimForCluster(ctx, ns, "missing-cluster", "e2e_missing_"+suffix, "missing-"+suffix, cnpgNamespace, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReadyReason(ctx, ns, "missing-cluster", controller.ReasonClusterMissing)
	})

	It("reports ClusterNotReady for an existing CNPG cluster before it is ready", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "reasons-notready")
		cluster := "e2e-notready-" + suffix
		createNotReadyCNPGCluster(ctx, cluster)

		createDBClaimForCluster(ctx, ns, "not-ready-cluster", "e2e_notready_"+suffix, cluster, cnpgNamespace, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReadyReason(ctx, ns, "not-ready-cluster", controller.ReasonClusterNotReady)
	})
})
