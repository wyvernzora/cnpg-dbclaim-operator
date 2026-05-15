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
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

var _ = Describe("permission narrowing", Ordered, func() {
	It("revokes previous grants before applying the narrowed per-schema grants", func() {
		ctx := context.Background()
		suffix := randomSuffix()
		ns := createNamespace(ctx, "narrow")
		dbName := "e2e_narrow_" + suffix
		roleName := "e2e_narrow_role_" + suffix

		createDBClaim(ctx, ns, "narrow-db", dbName, cnpgclaimv1alpha1.DeletionPolicyRetain, "app", "shared")
		waitDBReady(ctx, ns, "narrow-db")
		super, err := connectSuper(ctx, dbName)
		Expect(err).NotTo(HaveOccurred())
		defer super.Close(ctx)
		expectExec(ctx, super, `CREATE TABLE app.items (id int PRIMARY KEY)`)
		expectExec(ctx, super, `INSERT INTO app.items (id) VALUES (1)`)

		createExplicitRoleClaim(ctx, ns, "svc", "narrow-db", roleName,
			cnpgclaimv1alpha1.SchemaGrant{Name: "app", Access: cnpgclaimv1alpha1.AccessReadWrite},
			cnpgclaimv1alpha1.SchemaGrant{Name: "shared", Access: cnpgclaimv1alpha1.AccessReadOnly},
		)
		claim := waitRoleReady(ctx, ns, "svc")
		conn, err := connectAs(ctx, getSecret(ctx, ns, claim.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer conn.Close(ctx)
		expectExec(ctx, conn, fmt.Sprintf("INSERT INTO %s (id) VALUES (2)", quoteTable("app", "items")))

		var current cnpgclaimv1alpha1.RoleClaim
		Expect(k8sClient.Get(ctx, client.ObjectKey{Name: "svc", Namespace: ns}, &current)).To(Succeed())
		before := current.DeepCopy()
		current.Spec.Schemas = []cnpgclaimv1alpha1.SchemaGrant{
			{Name: "app", Access: cnpgclaimv1alpha1.AccessReadOnly},
			{Name: "shared", Access: cnpgclaimv1alpha1.AccessReadOnly},
		}
		Expect(k8sClient.Patch(ctx, &current, client.MergeFrom(before))).To(Succeed())
		waitRoleSchemaAccess(ctx, ns, "svc", "app", cnpgclaimv1alpha1.AccessReadOnly)

		expectQueryRow(ctx, conn, fmt.Sprintf("SELECT count(*) FROM %s", quoteTable("app", "items")))
		expectCannotInsert(ctx, conn, "app", "items")
	})
})
