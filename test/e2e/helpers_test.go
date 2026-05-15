//go:build e2e

/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cnpgclaimv1alpha1 "github.com/wyvernzora/cnpg-dbclaim-operator/api/v1alpha1"
	"github.com/wyvernzora/cnpg-dbclaim-operator/internal/postgres"
)

const (
	e2eTimeout      = 90 * time.Second
	e2eLongTimeout  = 180 * time.Second
	e2ePollInterval = 2 * time.Second
)

var forwardingRE = regexp.MustCompile(`Forwarding from 127\.0\.0\.1:(\d+) -> 5432`)

func portForwardPostgres(ctx context.Context, kubeconfig, ns, svc string) (int, func(), error) {
	ctx, cancel := context.WithCancel(ctx)
	args := []string{"--kubeconfig", kubeconfig, "-n", ns, "port-forward", "service/" + svc, ":5432"}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return 0, nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return 0, nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return 0, nil, err
	}

	lines := make(chan string, 16)
	readLines := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}
	go readLines(stdout)
	go readLines(stderr)

	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case line := <-lines:
			matches := forwardingRE.FindStringSubmatch(line)
			if len(matches) != 2 {
				continue
			}
			var port int
			if _, err := fmt.Sscanf(matches[1], "%d", &port); err != nil {
				continue
			}
			stop := func() {
				cancel()
				if cmd.Process != nil {
					_ = cmd.Process.Signal(syscall.SIGTERM)
				}
				_ = cmd.Wait()
			}
			return port, stop, nil
		case <-timeout.C:
			cancel()
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			_ = cmd.Wait()
			return 0, nil, fmt.Errorf("timed out waiting for kubectl port-forward to %s/%s", ns, svc)
		}
	}
}

func dsnFromSecret(s *corev1.Secret, localPort int) string {
	return dsnFromSecretForDB(s, localPort, string(s.Data["dbname"]))
}

func dsnFromSecretForDB(s *corev1.Secret, localPort int, dbname string) string {
	user := string(s.Data["user"])
	if user == "" {
		user = string(s.Data["username"])
	}
	if dbname == "" {
		dbname = postgres.AdminDatabase
	}
	u := url.URL{
		Scheme: "postgresql",
		User:   url.UserPassword(user, string(s.Data["password"])),
		Host:   fmt.Sprintf("127.0.0.1:%d", localPort),
		Path:   "/" + dbname,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	return u.String()
}

func connectAs(ctx context.Context, s *corev1.Secret, localPort int) (*pgx.Conn, error) {
	return pgx.Connect(ctx, dsnFromSecret(s, localPort))
}

func connectSuper(ctx context.Context, dbname string) (*pgx.Conn, error) {
	return pgx.Connect(ctx, dsnFromSecretForDB(superuserSecret(ctx), pgPort, dbname))
}

func randomSuffix() string {
	var b [4]byte
	_, err := rand.Read(b[:])
	Expect(err).NotTo(HaveOccurred())
	return hex.EncodeToString(b[:])
}

func createNamespace(ctx context.Context, base string) string {
	ns := strings.ToLower(fmt.Sprintf("e2e-%s-%s", base, randomSuffix()))
	Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
	DeferCleanup(func(ctx SpecContext) {
		cleanupNamespace(ctx, ns)
	}, NodeTimeout(70*time.Second))
	return ns
}

func cleanupNamespace(ctx context.Context, ns string) {
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	err := k8sClient.Delete(ctx, namespace)
	if err != nil && !apierrors.IsNotFound(err) {
		fmt.Fprintf(GinkgoWriter, "delete namespace %s: %v\n", ns, err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var got corev1.Namespace
		err := k8sClient.Get(ctx, client.ObjectKey{Name: ns}, &got)
		if apierrors.IsNotFound(err) {
			return
		}
		if err != nil {
			fmt.Fprintf(GinkgoWriter, "get namespace %s during cleanup: %v\n", ns, err)
			return
		}
		time.Sleep(2 * time.Second)
	}
	fmt.Fprintf(GinkgoWriter, "namespace %s cleanup timed out; dumping state\n", ns)
	dumpState(ctx)
}

func createDBClaim(ctx context.Context, ns, name, dbName string, deletion cnpgclaimv1alpha1.DeletionPolicy, schemas ...string) {
	claim := &cnpgclaimv1alpha1.DatabaseClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: cnpgclaimv1alpha1.DatabaseClaimSpec{
			DatabaseName:   dbName,
			ClusterRef:     cnpgclaimv1alpha1.ClusterReference{Name: clusterName, Namespace: cnpgNamespace},
			Schemas:        schemas,
			DeletionPolicy: deletion,
		},
	}
	Expect(k8sClient.Create(ctx, claim)).To(Succeed())
}

func createRoleClaim(ctx context.Context, ns, name, dbClaim, roleName string, access cnpgclaimv1alpha1.AccessLevel) {
	a := access
	claim := &cnpgclaimv1alpha1.RoleClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: cnpgclaimv1alpha1.RoleClaimSpec{
			DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: dbClaim},
			RoleName:         roleName,
			Access:           &a,
		},
	}
	Expect(k8sClient.Create(ctx, claim)).To(Succeed())
}

func createExplicitRoleClaim(ctx context.Context, ns, name, dbClaim, roleName string, grants ...cnpgclaimv1alpha1.SchemaGrant) {
	claim := &cnpgclaimv1alpha1.RoleClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: cnpgclaimv1alpha1.RoleClaimSpec{
			DatabaseClaimRef: cnpgclaimv1alpha1.DatabaseClaimRef{Name: dbClaim},
			RoleName:         roleName,
			Schemas:          grants,
		},
	}
	Expect(k8sClient.Create(ctx, claim)).To(Succeed())
}

func waitDBReady(ctx context.Context, ns, name string) cnpgclaimv1alpha1.DatabaseClaim {
	var got cnpgclaimv1alpha1.DatabaseClaim
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got)).To(Succeed())
		g.Expect(got.Status.Phase).To(Equal(cnpgclaimv1alpha1.DatabaseClaimPhaseReady))
	}, e2eTimeout, e2ePollInterval).Should(Succeed())
	return got
}

func waitRoleReady(ctx context.Context, ns, name string) cnpgclaimv1alpha1.RoleClaim {
	var got cnpgclaimv1alpha1.RoleClaim
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got)).To(Succeed())
		g.Expect(got.Status.Phase).To(Equal(cnpgclaimv1alpha1.RoleClaimPhaseReady))
		g.Expect(got.Status.CredentialsSecretName).NotTo(BeEmpty())
	}, e2eTimeout, e2ePollInterval).Should(Succeed())
	return got
}

func waitRoleSchemaAccess(ctx context.Context, ns, name, schema string, access cnpgclaimv1alpha1.AccessLevel) {
	Eventually(func(g Gomega) {
		var got cnpgclaimv1alpha1.RoleClaim
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got)).To(Succeed())
		g.Expect(got.Status.Phase).To(Equal(cnpgclaimv1alpha1.RoleClaimPhaseReady))
		found := false
		for _, grant := range got.Status.ResolvedSchemas {
			if grant.Name == schema {
				g.Expect(grant.Access).To(Equal(access))
				found = true
			}
		}
		g.Expect(found).To(BeTrue(), "schema %s not found in resolved schemas: %#v", schema, got.Status.ResolvedSchemas)
	}, e2eTimeout, e2ePollInterval).Should(Succeed())
}

func waitRoleSchemaAbsent(ctx context.Context, ns, name, schema string) {
	Eventually(func(g Gomega) {
		var got cnpgclaimv1alpha1.RoleClaim
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &got)).To(Succeed())
		g.Expect(got.Status.Phase).To(Equal(cnpgclaimv1alpha1.RoleClaimPhaseReady))
		for _, grant := range got.Status.ResolvedSchemas {
			g.Expect(grant.Name).NotTo(Equal(schema))
		}
	}, e2eTimeout, e2ePollInterval).Should(Succeed())
}

func getSecret(ctx context.Context, ns, name string) *corev1.Secret {
	var secret corev1.Secret
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, &secret)).To(Succeed())
	return &secret
}

func quoteTable(schemaName, tableName string) string {
	return pgx.Identifier{schemaName, tableName}.Sanitize()
}

func expectExec(ctx context.Context, conn *pgx.Conn, sql string, args ...any) {
	_, err := conn.Exec(ctx, sql, args...)
	Expect(err).NotTo(HaveOccurred())
}

func expectQueryRow(ctx context.Context, conn *pgx.Conn, sql string, args ...any) {
	var n int
	Expect(conn.QueryRow(ctx, sql, args...).Scan(&n)).To(Succeed())
}

func expectSQLState(err error, code string) {
	var pgErr *pgconn.PgError
	Expect(errors.As(err, &pgErr)).To(BeTrue(), "expected PostgreSQL error %s, got %v", code, err)
	Expect(pgErr.Code).To(Equal(code))
}

func expectCannotInsert(ctx context.Context, conn *pgx.Conn, schemaName, tableName string) {
	_, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (9999)", quoteTable(schemaName, tableName)))
	Expect(err).To(HaveOccurred())
	expectSQLState(err, "42501")
}

func expectCannotSelect(ctx context.Context, conn *pgx.Conn, schemaName, tableName string) {
	var n int
	err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", quoteTable(schemaName, tableName))).Scan(&n)
	Expect(err).To(HaveOccurred())
	expectSQLState(err, "42501")
}

func defaultACLReaders(ctx context.Context, conn *pgx.Conn, schemaName, writer string) []string {
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT reader.rolname
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		JOIN pg_roles writer ON writer.oid = d.defaclrole
		CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
		JOIN pg_roles reader ON reader.oid = acl.grantee
		WHERE n.nspname = $1
		  AND writer.rolname = $2
		  AND acl.privilege_type = 'SELECT'
		ORDER BY reader.rolname
	`, schemaName, writer)
	Expect(err).NotTo(HaveOccurred())
	defer rows.Close()
	var out []string
	for rows.Next() {
		var role string
		Expect(rows.Scan(&role)).To(Succeed())
		out = append(out, role)
	}
	Expect(rows.Err()).NotTo(HaveOccurred())
	return out
}

func databaseExists(ctx context.Context, conn *pgx.Conn, dbName string) bool {
	var exists bool
	Expect(conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, dbName).Scan(&exists)).To(Succeed())
	return exists
}

func loadAndPatchSamples(path string, mutate func(*unstructured.Unstructured)) ([]*unstructured.Unstructured, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewYAMLToJSONDecoder(bytes.NewReader(data))
	var out []*unstructured.Unstructured
	for {
		obj := &unstructured.Unstructured{}
		err := decoder.Decode(obj)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if obj.GetKind() == "" {
			continue
		}
		mutate(obj)
		out = append(out, obj)
	}
	return out, nil
}

func patchSampleObject(obj *unstructured.Unstructured, ns, suffix, dbName string) {
	obj.SetNamespace(ns)
	switch obj.GroupVersionKind().GroupKind() {
	case schema.GroupKind{Group: "cnpg.wyvernzora.io", Kind: "DatabaseClaim"}:
		_, _, _ = unstructured.NestedMap(obj.Object, "spec", "clusterRef")
		_ = unstructured.SetNestedField(obj.Object, cnpgNamespace, "spec", "clusterRef", "namespace")
		_ = unstructured.SetNestedField(obj.Object, clusterName, "spec", "clusterRef", "name")
		_ = unstructured.SetNestedField(obj.Object, dbName, "spec", "databaseName")
	case schema.GroupKind{Group: "cnpg.wyvernzora.io", Kind: "RoleClaim"}:
		roleName := strings.ReplaceAll(obj.GetName(), "-", "_") + "_" + suffix
		_ = unstructured.SetNestedField(obj.Object, roleName, "spec", "roleName")
	}
}

func dumpState(ctx context.Context) {
	artifactDir := filepath.Join("test", "e2e", "_artifacts", time.Now().Format("20060102-150405"))
	_ = os.MkdirAll(artifactDir, 0o755)
	runAndWrite := func(name string, args ...string) {
		cmd := exec.CommandContext(ctx, name, args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Fprintf(GinkgoWriter, "%s %s: %v\n", name, strings.Join(args, " "), err)
		}
		fmt.Fprintf(GinkgoWriter, "===== %s %s =====\n%s\n", name, strings.Join(args, " "), out)
		file := filepath.Join(artifactDir, strings.NewReplacer("/", "_", " ", "_", "-", "_").Replace(name+"_"+strings.Join(args, "_"))+".txt")
		_ = os.WriteFile(file, out, 0o644)
	}
	runAndWrite("kubectl", "get", "pods,svc,roleclaims,databaseclaims", "-A")
	runAndWrite("kubectl", "get", "roleclaims,databaseclaims", "-A", "-o", "yaml")
	runAndWrite("kubectl", "-n", operatorNamespace, "logs", "-l", "app.kubernetes.io/name=dbclaim-operator", "--tail=500")
	runAndWrite("kubectl", "-n", cnpgNamespace, "describe", "clusters.postgresql.cnpg.io/"+clusterName)
	runAndWrite("kubectl", "get", "events", "-A", "--sort-by=.lastTimestamp")

	conn, err := connectSuper(ctx, postgres.AdminDatabase)
	if err != nil {
		fmt.Fprintf(GinkgoWriter, "connect superuser for catalog dump: %v\n", err)
		return
	}
	defer conn.Close(ctx)
	dumpCatalog(ctx, conn, artifactDir, "databases", `SELECT datname FROM pg_database WHERE datname LIKE 'e2e_%' ORDER BY datname`)
	dumpCatalog(ctx, conn, artifactDir, "roles", `SELECT rolname FROM pg_roles WHERE rolname LIKE 'e2e_%' ORDER BY rolname`)
}

func dumpCatalog(ctx context.Context, conn *pgx.Conn, artifactDir, name, query string) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		fmt.Fprintf(GinkgoWriter, "catalog dump %s: %v\n", name, err)
		return
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err == nil {
			values = append(values, value)
		}
	}
	slices.Sort(values)
	out := []byte(strings.Join(values, "\n"))
	fmt.Fprintf(GinkgoWriter, "===== pg_%s =====\n%s\n", name, out)
	_ = os.WriteFile(filepath.Join(artifactDir, "pg_"+name+".txt"), out, 0o644)
}
