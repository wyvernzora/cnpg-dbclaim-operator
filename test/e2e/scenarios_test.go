//go:build e2e

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("sample scenarios", Ordered, func() {
	It("applies scenario B bounded-context manifests", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "scenario-b")
		dbName := "e2e_scenariob_" + suffix

		applyPatchedSample(ctx, "../../config/samples/scenario_b_bounded_context.yaml", ns, suffix, map[string]string{
			"orders-domain": dbName,
		})

		waitDBReady(ctx, ns, "orders-domain")
		ordering := waitRoleReady(ctx, ns, "ordering-svc")
		shipping := waitRoleReady(ctx, ns, "shipping-svc")
		bi := waitRoleReady(ctx, ns, "bi-reader")

		orderingConn, err := connectAs(ctx, getSecret(ctx, ns, ordering.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer orderingConn.Close(ctx)
		expectExec(ctx, orderingConn, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", quoteTable("ordering", "orders")))
		expectExec(ctx, orderingConn, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", quoteTable("shared", "ordering_events")))

		shippingConn, err := connectAs(ctx, getSecret(ctx, ns, shipping.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer shippingConn.Close(ctx)
		expectExec(ctx, shippingConn, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", quoteTable("shipping", "shipments")))

		biConn, err := connectAs(ctx, getSecret(ctx, ns, bi.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer biConn.Close(ctx)
		expectQueryRow(ctx, biConn, fmt.Sprintf("SELECT count(*) FROM %s", quoteTable("ordering", "orders")))
		expectQueryRow(ctx, biConn, fmt.Sprintf("SELECT count(*) FROM %s", quoteTable("shipping", "shipments")))
		expectCannotInsert(ctx, biConn, "ordering", "orders")
	})

	It("applies scenario C independent-app manifests", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "scenario-c")

		applyPatchedSample(ctx, "../../config/samples/scenario_c_independent_apps.yaml", ns, suffix, map[string]string{
			"agent":     "e2e_agent_" + suffix,
			"scheduler": "e2e_scheduler_" + suffix,
		})

		waitDBReady(ctx, ns, "agent")
		waitDBReady(ctx, ns, "scheduler")
		agent := waitRoleReady(ctx, ns, "agent")
		scheduler := waitRoleReady(ctx, ns, "scheduler")

		agentConn, err := connectAs(ctx, getSecret(ctx, ns, agent.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer agentConn.Close(ctx)
		expectExec(ctx, agentConn, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", quoteTable("app", "agent_items")))

		schedulerConn, err := connectAs(ctx, getSecret(ctx, ns, scheduler.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer schedulerConn.Close(ctx)
		expectExec(ctx, schedulerConn, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", quoteTable("app", "scheduler_items")))
	})
})
