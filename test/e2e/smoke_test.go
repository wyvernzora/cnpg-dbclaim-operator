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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
)

var _ = Describe("smoke", Ordered, func() {
	var (
		ctx    context.Context
		ns     string
		suffix string
		dbName string
	)

	BeforeAll(func() {
		ctx = context.Background()
		suffix = randomSuffix()
		ns = createNamespace(ctx, "smoke")
		dbName = "e2e_smoke_" + suffix
	})

	It("rejects a RoleClaim without spec.roleName", func() {
		obj := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "cnpg.wyvernzora.io/v1alpha1",
			"kind":       "RoleClaim",
			"metadata": map[string]any{
				"name":      "missing-role-name",
				"namespace": ns,
			},
			"spec": map[string]any{
				"databaseClaimRef": map[string]any{"name": "missing"},
				"access":           string(cnpgclaimv1alpha1.AccessReadOnly),
			},
		}}
		err := k8sClient.Create(ctx, obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("roleName"))
	})

	It("provisions a database and three roles with expected SQL privileges", func() {
		createDBClaim(ctx, ns, "smoke-db", dbName, cnpgclaimv1alpha1.DeletionPolicyRetain, "app")
		waitDBReady(ctx, ns, "smoke-db")

		super, err := connectSuper(ctx, dbName)
		Expect(err).NotTo(HaveOccurred())
		defer super.Close(ctx)
		expectExec(ctx, super, `CREATE TABLE app.smoke_items (id int PRIMARY KEY)`)
		expectExec(ctx, super, `INSERT INTO app.smoke_items (id) VALUES (1)`)

		ownerRole := "e2e_smoke_owner_" + suffix
		rwRole := "e2e_smoke_rw_" + suffix
		roRole := "e2e_smoke_ro_" + suffix
		createRoleClaim(ctx, ns, "owner", "smoke-db", ownerRole, cnpgclaimv1alpha1.AccessOwner)
		createRoleClaim(ctx, ns, "rw", "smoke-db", rwRole, cnpgclaimv1alpha1.AccessReadWrite)
		createRoleClaim(ctx, ns, "ro", "smoke-db", roRole, cnpgclaimv1alpha1.AccessReadOnly)
		ownerClaim := waitRoleReady(ctx, ns, "owner")
		rwClaim := waitRoleReady(ctx, ns, "rw")
		roClaim := waitRoleReady(ctx, ns, "ro")

		owner, err := connectAs(ctx, getSecret(ctx, ns, ownerClaim.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer owner.Close(ctx)
		expectExec(ctx, owner, fmt.Sprintf("CREATE TABLE %s (id int)", quoteTable("app", "owner_items")))
		expectExec(ctx, owner, fmt.Sprintf("ALTER TABLE %s ADD COLUMN note text", quoteTable("app", "owner_items")))

		rw, err := connectAs(ctx, getSecret(ctx, ns, rwClaim.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer rw.Close(ctx)
		expectExec(ctx, rw, fmt.Sprintf("INSERT INTO %s (id) VALUES (2)", quoteTable("app", "smoke_items")))

		ro, err := connectAs(ctx, getSecret(ctx, ns, roClaim.Status.CredentialsSecretName), pgPort)
		Expect(err).NotTo(HaveOccurred())
		defer ro.Close(ctx)
		expectQueryRow(ctx, ro, fmt.Sprintf("SELECT count(*) FROM %s", quoteTable("app", "smoke_items")))
		expectCannotInsert(ctx, ro, "app", "smoke_items")
	})

	It("loads and patches sample manifests as typed YAML objects", func() {
		objects, err := loadAndPatchSamples("../../config/samples/scenario_a_simple.yaml", func(obj *unstructured.Unstructured) {
			patchSampleObject(obj, ns, suffix, dbName)
			obj.SetCreationTimestamp(metav1.Time{})
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(objects).To(HaveLen(4))
	})
})
