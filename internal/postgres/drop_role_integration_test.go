/*
Copyright 2026 contributors to cnpg-dbclaim-operator.
*/

package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestDropRoleRemovesDefaultACLReferences(t *testing.T) {
	dsn := os.Getenv("CNPG_DBCLAIM_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set CNPG_DBCLAIM_POSTGRES_DSN to run default-ACL cleanup integration test against a real Postgres")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer conn.Close(ctx)

	suffix := strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
	writer := "acl_writer_" + suffix
	reader := "acl_reader_" + suffix
	schema := "acl_schema_" + suffix

	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN", Quote(writer)),
		fmt.Sprintf("CREATE ROLE %s LOGIN", Quote(reader)),
		fmt.Sprintf("CREATE SCHEMA %s AUTHORIZATION %s", Quote(schema), Quote(writer)),
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup %q: %v", stmt, err)
		}
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", Quote(schema)))
		_, _ = conn.Exec(context.Background(), fmt.Sprintf("DROP ROLE IF EXISTS %s", Quote(reader)))
		_, _ = conn.Exec(context.Background(), fmt.Sprintf("DROP ROLE IF EXISTS %s", Quote(writer)))
	}()

	if err := AlterDefaultPrivilegesGrantSelect(ctx, conn, writer, reader, schema); err != nil {
		t.Fatalf("grant default privileges: %v", err)
	}
	before := defaultACLReferencesRole(ctx, t, conn, schema, reader)
	if before == 0 {
		t.Fatal("expected pg_default_acl to reference reader before DropRole")
	}

	if err := DropRole(ctx, conn, reader, "postgres"); err != nil {
		t.Fatalf("drop reader role: %v", err)
	}
	after := defaultACLReferencesRole(ctx, t, conn, schema, reader)
	if after != 0 {
		t.Fatalf("expected pg_default_acl references to reader to be removed, got %d", after)
	}
}

func defaultACLReferencesRole(ctx context.Context, t *testing.T, conn *pgx.Conn, schema, role string) int {
	t.Helper()
	var count int
	err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		WHERE n.nspname = $1
		  AND d.defaclacl::text LIKE '%' || $2 || '%'
	`, schema, role).Scan(&count)
	if err != nil {
		t.Fatalf("query pg_default_acl: %v", err)
	}
	return count
}
